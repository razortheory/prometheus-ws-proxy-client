use crate::BoxError;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Target {
    ws_url: Url,
    response_base: Url,
}

impl Target {
    pub fn parse(value: &str) -> Result<Self, BoxError> {
        let mut source = Url::parse(value)?;
        let (ws_scheme, http_scheme) = match source.scheme() {
            "http" => ("ws", "http"),
            "https" => ("wss", "https"),
            "ws" => ("ws", "http"),
            "wss" => ("wss", "https"),
            scheme => return Err(format!("unsupported target URL scheme: {scheme}").into()),
        };

        source.set_fragment(None);
        source.set_query(None);
        let trimmed = source.path().trim_end_matches('/');
        let (base_path, ws_path) = if trimmed.ends_with("/ws") || trimmed == "ws" {
            let base = trimmed.strip_suffix("/ws").unwrap_or("");
            (with_trailing_slash(base), with_trailing_slash(trimmed))
        } else {
            let base = with_trailing_slash(trimmed);
            let ws = format!("{base}ws/");
            (base, ws)
        };

        let mut ws_url = source.clone();
        ws_url
            .set_scheme(ws_scheme)
            .map_err(|_| "invalid WS scheme")?;
        ws_url.set_path(&ws_path);

        let mut response_base = source;
        response_base
            .set_scheme(http_scheme)
            .map_err(|_| "invalid HTTP scheme")?;
        response_base.set_path(&base_path);

        Ok(Self {
            ws_url,
            response_base,
        })
    }

    pub fn ws_url(&self) -> &Url {
        &self.ws_url
    }

    pub fn response_url(&self, uid: &str) -> Result<Url, BoxError> {
        let mut result = self.response_base.clone();
        result
            .path_segments_mut()
            .map_err(|_| "target URL cannot be a base")?
            .pop_if_empty()
            .push("response")
            .push(uid)
            .push("");
        Ok(result)
    }

    pub fn response_base(&self) -> &Url {
        &self.response_base
    }
}

fn with_trailing_slash(path: &str) -> String {
    let path = if path.is_empty() { "/" } else { path };
    if path.ends_with('/') {
        path.to_owned()
    } else {
        format!("{path}/")
    }
}

#[cfg(test)]
mod tests {
    use super::Target;

    #[test]
    fn normalizes_http_bases_and_ready_ws_urls() {
        let cases = [
            (
                "https://example.test/proxy/",
                "wss://example.test/proxy/ws/",
                "https://example.test/proxy/",
            ),
            (
                "http://example.test/proxy",
                "ws://example.test/proxy/ws/",
                "http://example.test/proxy/",
            ),
            (
                "wss://example.test/proxy2/ws/",
                "wss://example.test/proxy2/ws/",
                "https://example.test/proxy2/",
            ),
            (
                "ws://example.test/proxy2/ws",
                "ws://example.test/proxy2/ws/",
                "http://example.test/proxy2/",
            ),
        ];

        for (input, ws, base) in cases {
            let target = Target::parse(input).unwrap();
            assert_eq!(target.ws_url().as_str(), ws);
            assert_eq!(target.response_base().as_str(), base);
        }
    }

    #[test]
    fn response_uid_is_encoded_as_one_path_segment() {
        let target = Target::parse("https://example.test/a/").unwrap();
        assert_eq!(
            target.response_url("id/with space").unwrap().as_str(),
            "https://example.test/a/response/id%2Fwith%20space/"
        );
    }

    #[test]
    fn rejects_non_http_websocket_schemes() {
        assert!(Target::parse("file:///tmp/socket").is_err());
    }
}
