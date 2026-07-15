use crate::config::Config;
use crate::protocol::ResourceResponse;
use crate::MAX_BODY_SIZE;
use futures_util::StreamExt;
use reqwest::header::CONTENT_LENGTH;
use std::sync::Arc;

pub async fn call_resource(
    client: &reqwest::Client,
    config: &Config,
    uid: String,
    resource: &str,
) -> ResourceResponse {
    let Some(url) = config.resources.get(resource) else {
        tracing::warn!(resource, "unknown resource requested");
        return ResourceResponse::new(uid, 404, "No such resource".to_owned());
    };

    match fetch_resource(client, url).await {
        Ok((status, body)) => ResourceResponse::new(uid, status, body),
        Err(error) => {
            tracing::warn!(resource, error = %error, "resource request failed");
            ResourceResponse::new(uid, 500, String::new())
        }
    }
}

async fn fetch_resource(
    client: &reqwest::Client,
    url: &str,
) -> Result<(u16, String), crate::BoxError> {
    let response = client.get(url).send().await?;
    let status = response.status().as_u16();
    let declared_length = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared_length.is_some_and(|length| length > MAX_BODY_SIZE as u64) {
        return Err("resource response exceeds the 64 MiB limit".into());
    }

    let mut bytes = Vec::with_capacity(
        declared_length
            .unwrap_or_default()
            .min(MAX_BODY_SIZE as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > MAX_BODY_SIZE {
            return Err("resource response exceeds the 64 MiB limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = String::from_utf8(bytes)?;
    Ok((status, body))
}

pub fn shared_http_client() -> Result<Arc<reqwest::Client>, reqwest::Error> {
    crate::install_rustls_provider();
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(60))
        .user_agent(concat!("proxy-client/", env!("CARGO_PKG_VERSION")))
        .build()
        .map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::call_resource;
    use crate::config::Config;
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use std::collections::HashMap;

    fn config(resources: HashMap<String, String>) -> Config {
        Config {
            instance: "test".into(),
            target: "http://example.test/".into(),
            resources,
            cf_access_enabled: false,
            cf_access_key: String::new(),
            cf_access_secret: String::new(),
            ec2_meta_domain: String::new(),
        }
    }

    async fn serve(app: Router) -> String {
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
    async fn unknown_resource_is_exact_404() {
        let response =
            call_resource(&client(), &config(HashMap::new()), "uid".into(), "missing").await;
        assert_eq!(response.status, 404);
        assert_eq!(response.body, "No such resource");
    }

    #[tokio::test]
    async fn preserves_exporter_status_and_body() {
        let base = serve(Router::new().route(
            "/metrics",
            get(|| async { (StatusCode::CREATED, "metric 1\n") }),
        ))
        .await;
        let response = call_resource(
            &client(),
            &config(HashMap::from([("node".into(), format!("{base}/metrics"))])),
            "uid".into(),
            "node",
        )
        .await;
        assert_eq!(response.status, 201);
        assert_eq!(response.body, "metric 1\n");
    }

    #[tokio::test]
    async fn rejects_content_length_over_limit_without_reading_body() {
        let base = serve(Router::new().route(
            "/oversize",
            get(|| async {
                Response::builder()
                    .header("content-length", (crate::MAX_BODY_SIZE + 1).to_string())
                    .body(Body::from("x"))
                    .unwrap()
            }),
        ))
        .await;
        let response = call_resource(
            &client(),
            &config(HashMap::from([("node".into(), format!("{base}/oversize"))])),
            "uid".into(),
            "node",
        )
        .await;
        assert_eq!(response.status, 500);
        assert!(response.body.is_empty());
    }

    #[tokio::test]
    async fn invalid_utf8_is_a_bounded_500_not_a_panic() {
        let base = serve(Router::new().route(
            "/invalid",
            get(|| async { Response::new(Body::from(vec![0xff, 0xfe])) }),
        ))
        .await;
        let response = call_resource(
            &client(),
            &config(HashMap::from([("node".into(), format!("{base}/invalid"))])),
            "uid".into(),
            "node",
        )
        .await;
        assert_eq!(response.status, 500);
        assert!(response.body.is_empty());
    }
}
