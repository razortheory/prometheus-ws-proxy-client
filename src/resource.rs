use crate::config::Config;
use crate::memory::{BudgetedResponse, MemoryBudget, MemoryReservation};
use crate::MAX_BODY_SIZE;
use futures_util::StreamExt;
use reqwest::header::CONTENT_LENGTH;
use std::sync::Arc;

pub async fn call_resource(
    client: &reqwest::Client,
    config: &Config,
    uid: String,
    resource: &str,
    memory_budget: &MemoryBudget,
) -> Option<BudgetedResponse> {
    let uid_bytes = uid.capacity();
    let mut retained = match memory_budget.try_reserve(uid_bytes) {
        Ok(retained) => retained,
        Err(error) => {
            tracing::warn!(uid_length = uid_bytes, error = %error, "resource request rejected by memory budget");
            return None;
        }
    };

    let Some(url) = config.resources.get(resource) else {
        tracing::warn!(resource, "unknown resource requested");
        let body = "No such resource";
        if retained.try_grow(body.len()).is_ok() {
            return Some(BudgetedResponse::from_reserved(
                uid,
                404,
                body.to_owned(),
                retained,
            ));
        }
        return Some(BudgetedResponse::from_reserved(
            uid,
            500,
            String::new(),
            retained,
        ));
    };

    match fetch_resource(client, url, &mut retained).await {
        Ok((status, body)) => Some(BudgetedResponse::from_reserved(uid, status, body, retained)),
        Err(error) => {
            tracing::warn!(resource, error = %error, "resource request failed");
            retained.shrink_to(uid_bytes);
            Some(BudgetedResponse::from_reserved(
                uid,
                500,
                String::new(),
                retained,
            ))
        }
    }
}

async fn fetch_resource(
    client: &reqwest::Client,
    url: &str,
    retained: &mut MemoryReservation,
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

    let initial_capacity = declared_length
        .unwrap_or_default()
        .min(MAX_BODY_SIZE as u64) as usize;
    retained.try_grow(initial_capacity)?;
    let mut bytes = Vec::with_capacity(initial_capacity);
    if bytes.capacity() > initial_capacity {
        retained.try_grow(bytes.capacity() - initial_capacity)?;
    }
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next_length = bytes.len().saturating_add(chunk.len());
        if next_length > MAX_BODY_SIZE {
            return Err("resource response exceeds the 64 MiB limit".into());
        }
        let previous_capacity = bytes.capacity();
        if next_length > previous_capacity {
            let minimum_growth = next_length - previous_capacity;
            retained.try_grow(minimum_growth)?;
            bytes.try_reserve_exact(chunk.len())?;
            let actual_growth = bytes.capacity() - previous_capacity;
            if actual_growth > minimum_growth {
                retained.try_grow(actual_growth - minimum_growth)?;
            }
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
    use crate::memory::MemoryBudget;
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

    fn budget() -> MemoryBudget {
        MemoryBudget::new(crate::memory::DEFAULT_MEMORY_BUDGET_SIZE)
    }

    #[tokio::test]
    async fn unknown_resource_is_exact_404() {
        let response = call_resource(
            &client(),
            &config(HashMap::new()),
            "uid".into(),
            "missing",
            &budget(),
        )
        .await
        .unwrap();
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
            &budget(),
        )
        .await
        .unwrap();
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
            &budget(),
        )
        .await
        .unwrap();
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
            &budget(),
        )
        .await
        .unwrap();
        assert_eq!(response.status, 500);
        assert!(response.body.is_empty());
    }

    #[tokio::test]
    async fn retained_raw_bodies_share_budget_and_release_exact_bytes() {
        let base = serve(
            Router::new()
                .route("/bounded", get(|| async { "12345678" }))
                .route("/empty", get(|| async { "" })),
        )
        .await;
        let config = config(HashMap::from([
            ("node".into(), format!("{base}/bounded")),
            ("empty".into(), format!("{base}/empty")),
        ]));
        let budget = MemoryBudget::new(14);

        let first = call_resource(&client(), &config, "one".into(), "node", &budget)
            .await
            .unwrap();
        assert_eq!(first.status, 200);
        assert_eq!(first.body, "12345678");
        assert_eq!(budget.used(), 11);

        let second = call_resource(&client(), &config, "two".into(), "node", &budget)
            .await
            .unwrap();
        assert_eq!(second.status, 500);
        assert!(second.body.is_empty());
        assert_eq!(budget.used(), 14);

        drop(second);
        assert_eq!(budget.used(), 11);
        let small = call_resource(&client(), &config, "tri".into(), "empty", &budget)
            .await
            .unwrap();
        assert_eq!(small.status, 200);
        assert!(small.body.is_empty());
        assert_eq!(budget.used(), 14);
        drop(small);
        drop(first);
        assert_eq!(budget.used(), 0);

        let third = call_resource(&client(), &config, "tri".into(), "node", &budget)
            .await
            .unwrap();
        assert_eq!(third.status, 200);
        assert_eq!(budget.used(), 11);
    }
}
