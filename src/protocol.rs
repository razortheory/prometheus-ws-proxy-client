use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IncomingMessage {
    Ping,
    Pong,
    Ready {
        uid: String,
    },
    Request {
        uid: String,
        resource: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Serialize)]
struct RegisterV1<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    instance: &'a str,
}

#[derive(Serialize)]
struct RegisterV2<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    instance: &'a str,
    worker: &'a str,
    version: u8,
}

#[derive(Serialize)]
struct Ready<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    uid: &'a str,
    worker: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResourceResponse {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub uid: String,
    pub body: String,
    pub status: u16,
}

impl ResourceResponse {
    pub fn new(uid: String, status: u16, body: String) -> Self {
        Self {
            kind: "response",
            uid,
            body,
            status,
        }
    }
}

pub fn register_json(protocol: u8, instance: &str, worker: &str) -> serde_json::Result<String> {
    if protocol == 1 {
        serde_json::to_string(&RegisterV1 {
            kind: "register",
            instance,
        })
    } else {
        serde_json::to_string(&RegisterV2 {
            kind: "register",
            instance,
            worker,
            version: protocol,
        })
    }
}

pub fn ready_json(uid: &str, worker: &str) -> serde_json::Result<String> {
    serde_json::to_string(&Ready {
        kind: "ready",
        uid,
        worker,
    })
}

pub fn response_json(response: &ResourceResponse) -> serde_json::Result<String> {
    serde_json::to_string(response)
}

pub const PING_JSON: &str = r#"{"type":"ping"}"#;
pub const PONG_JSON: &str = r#"{"type":"pong"}"#;

#[cfg(test)]
mod tests {
    use super::{ready_json, register_json, response_json, ResourceResponse};
    use serde_json::json;

    #[test]
    fn serializes_exact_wire_register_variants() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&register_json(1, "i", "w").unwrap())
                .unwrap(),
            json!({"type": "register", "instance": "i"})
        );
        for version in [2, 3] {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(
                    &register_json(version, "i", "w").unwrap()
                )
                .unwrap(),
                json!({"type": "register", "instance": "i", "worker": "w", "version": version})
            );
        }
    }

    #[test]
    fn serializes_ready_and_ws_response() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&ready_json("u", "w").unwrap()).unwrap(),
            json!({"type": "ready", "uid": "u", "worker": "w"})
        );
        let response = ResourceResponse::new("u".into(), 201, "ok".into());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response_json(&response).unwrap()).unwrap(),
            json!({"type": "response", "uid": "u", "status": 201, "body": "ok"})
        );
    }
}
