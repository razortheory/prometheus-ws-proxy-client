use crate::BoxError;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

const DEFAULT_EC2_METADATA_DOMAIN: &str = "http://169.254.169.254";

#[derive(Clone, Deserialize)]
pub struct Config {
    pub instance: String,
    pub target: String,
    pub resources: HashMap<String, String>,
    #[serde(default)]
    pub cf_access_enabled: bool,
    #[serde(default)]
    pub cf_access_key: String,
    #[serde(default)]
    pub cf_access_secret: String,
    #[serde(default = "default_ec2_metadata_domain")]
    pub ec2_meta_domain: String,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("instance", &self.instance)
            .field("target", &self.target)
            .field("resources", &self.resources.keys().collect::<Vec<_>>())
            .field("cf_access_enabled", &self.cf_access_enabled)
            .field("cf_access_key", &"[redacted]")
            .field("cf_access_secret", &"[redacted]")
            .field("ec2_meta_domain", &self.ec2_meta_domain)
            .finish()
    }
}

fn default_ec2_metadata_domain() -> String {
    DEFAULT_EC2_METADATA_DOMAIN.to_owned()
}

impl Config {
    pub async fn load(path: &Path, metadata_client: &reqwest::Client) -> Result<Self, BoxError> {
        let contents = tokio::fs::read(path).await?;
        let mut config: Self = serde_json::from_slice(&contents)?;
        config.resolve_instance(metadata_client).await?;
        config.validate_protocol_identifiers()?;
        Ok(config)
    }

    pub async fn from_json(
        contents: &[u8],
        metadata_client: &reqwest::Client,
    ) -> Result<Self, BoxError> {
        let mut config: Self = serde_json::from_slice(contents)?;
        config.resolve_instance(metadata_client).await?;
        config.validate_protocol_identifiers()?;
        Ok(config)
    }

    fn validate_protocol_identifiers(&self) -> Result<(), BoxError> {
        crate::protocol::validate_instance(&self.instance)?;
        for resource in self.resources.keys() {
            crate::protocol::validate_resource(resource)?;
        }
        Ok(())
    }

    async fn resolve_instance(&mut self, client: &reqwest::Client) -> Result<(), BoxError> {
        if self.instance != "ec2" {
            return Ok(());
        }

        let base = self.ec2_meta_domain.trim_end_matches('/');
        let token_url = format!("{base}/latest/api/token");
        let instance_url = format!("{base}/latest/meta-data/instance-id");

        let token = client
            .put(token_url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
            .ok()
            .filter(|response| response.status().is_success());

        let token = match token {
            Some(response) => response.text().await.ok().filter(|value| !value.is_empty()),
            None => None,
        };

        let mut request = client.get(&instance_url);
        if let Some(token) = token.as_ref() {
            request = request.header("X-aws-ec2-metadata-token", HeaderValue::from_str(token)?);
        }

        let mut response = request.send().await?;
        if !response.status().is_success() && token.is_some() {
            response = client.get(&instance_url).send().await?;
        }
        if !response.status().is_success() {
            return Err(format!(
                "EC2 metadata returned status {} while resolving instance",
                response.status()
            )
            .into());
        }

        let instance = response.text().await?;
        let instance = instance.trim();
        if instance.is_empty() {
            return Err("EC2 metadata returned an empty instance id".into());
        }
        self.instance = instance.to_owned();
        Ok(())
    }
}

pub fn metadata_client() -> Result<reqwest::Client, reqwest::Error> {
    crate::install_rustls_provider();
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(5))
        .build()
}

#[cfg(test)]
mod tests {
    use super::Config;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, put};
    use axum::Router;
    use std::sync::{Arc, Mutex};

    async fn test_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    fn client() -> reqwest::Client {
        crate::install_rustls_provider();
        reqwest::Client::new()
    }

    #[tokio::test]
    async fn applies_defaults_for_static_instance() {
        let client = client();
        let config = Config::from_json(
            br#"{"instance":"host","target":"https://example.test/proxy/","resources":{}}"#,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(config.instance, "host");
        assert!(!config.cf_access_enabled);
        assert_eq!(config.cf_access_key, "");
        assert_eq!(config.cf_access_secret, "");
    }

    #[tokio::test]
    async fn rejects_oversized_protocol_identifiers() {
        let oversized_instance = "i".repeat(257);
        let instance_json = serde_json::json!({
            "instance": oversized_instance,
            "target": "https://example.test/proxy/",
            "resources": {}
        });
        assert!(
            Config::from_json(instance_json.to_string().as_bytes(), &client())
                .await
                .is_err()
        );

        let oversized_resource = "r".repeat(513);
        let resource_json = serde_json::json!({
            "instance": "host",
            "target": "https://example.test/proxy/",
            "resources": { oversized_resource: "http://127.0.0.1:9100/metrics" }
        });
        assert!(
            Config::from_json(resource_json.to_string().as_bytes(), &client())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn resolves_ec2_once_with_imdsv2() {
        let token_headers = Arc::new(Mutex::new(Vec::<HeaderMap>::new()));
        let app = Router::new()
            .route("/latest/api/token", put(|| async { "token" }))
            .route(
                "/latest/meta-data/instance-id",
                get(
                    |State(headers): State<Arc<Mutex<Vec<HeaderMap>>>>,
                     request_headers: HeaderMap| async move {
                        headers.lock().unwrap().push(request_headers.clone());
                        if request_headers
                            .get("X-aws-ec2-metadata-token")
                            .and_then(|value| value.to_str().ok())
                            == Some("token")
                        {
                            (StatusCode::OK, "i-test")
                        } else {
                            (StatusCode::UNAUTHORIZED, "missing token")
                        }
                    },
                ),
            )
            .with_state(token_headers.clone());
        let domain = test_server(app).await;
        let json = format!(
            r#"{{"instance":"ec2","target":"http://example.test/","resources":{{}},"ec2_meta_domain":"{domain}"}}"#
        );
        crate::install_rustls_provider();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let config = Config::from_json(json.as_bytes(), &client).await.unwrap();
        assert_eq!(config.instance, "i-test");
        assert_eq!(token_headers.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn falls_back_to_imdsv1() {
        let app = Router::new()
            .route(
                "/latest/api/token",
                put(|| async { (StatusCode::NOT_FOUND, "") }),
            )
            .route(
                "/latest/meta-data/instance-id",
                get(|headers: HeaderMap| async move {
                    assert!(headers.get("X-aws-ec2-metadata-token").is_none());
                    "i-v1"
                }),
            );
        let domain = test_server(app).await;
        let json = format!(
            r#"{{"instance":"ec2","target":"http://example.test/","resources":{{}},"ec2_meta_domain":"{domain}"}}"#
        );
        let config = Config::from_json(json.as_bytes(), &client()).await.unwrap();
        assert_eq!(config.instance, "i-v1");
    }
}
