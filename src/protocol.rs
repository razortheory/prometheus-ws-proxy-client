use serde::{Deserialize, Serialize};
use std::fmt;

pub const MAX_UID_SIZE: usize = 128;
pub const MAX_RESOURCE_SIZE: usize = 512;
pub const MAX_INSTANCE_SIZE: usize = 256;
pub const MAX_WORKER_SIZE: usize = 256;

#[derive(Debug, Eq, PartialEq)]
pub struct FieldLimitError {
    field: &'static str,
    length: usize,
    limit: usize,
}

impl fmt::Display for FieldLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is {} bytes, exceeding the {} byte limit",
            self.field, self.length, self.limit
        )
    }
}

impl std::error::Error for FieldLimitError {}

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

impl IncomingMessage {
    pub fn validate(&self) -> Result<(), FieldLimitError> {
        match self {
            Self::Ready { uid } => validate_uid(uid),
            Self::Request { uid, resource } => {
                validate_uid(uid)?;
                validate_resource(resource)
            }
            Self::Ping | Self::Pong | Self::Unknown => Ok(()),
        }
    }
}

pub fn validate_uid(uid: &str) -> Result<(), FieldLimitError> {
    validate_field("uid", uid, MAX_UID_SIZE)
}

pub fn validate_resource(resource: &str) -> Result<(), FieldLimitError> {
    validate_field("resource", resource, MAX_RESOURCE_SIZE)
}

pub fn validate_instance(instance: &str) -> Result<(), FieldLimitError> {
    validate_field("instance", instance, MAX_INSTANCE_SIZE)
}

pub fn validate_worker(worker: &str) -> Result<(), FieldLimitError> {
    validate_field("worker", worker, MAX_WORKER_SIZE)
}

fn validate_field(field: &'static str, value: &str, limit: usize) -> Result<(), FieldLimitError> {
    if value.len() <= limit {
        Ok(())
    } else {
        Err(FieldLimitError {
            field,
            length: value.len(),
            limit,
        })
    }
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
    let capacity = response_json_encoded_len(response).unwrap_or_default();
    let mut bytes = Vec::with_capacity(capacity);
    serde_json::to_writer(&mut bytes, response)?;
    debug_assert_eq!(bytes.len(), capacity);
    Ok(String::from_utf8(bytes).expect("JSON serialization always produces UTF-8"))
}

pub(crate) fn response_json_encoded_len(response: &ResourceResponse) -> Option<usize> {
    const PREFIX: &str = r#"{"type":"response","uid":""#;
    const BETWEEN_STRINGS: &str = r#"","body":""#;
    const BEFORE_STATUS: &str = r#"","status":"#;

    PREFIX
        .len()
        .checked_add(json_string_content_encoded_len(&response.uid)?)?
        .checked_add(BETWEEN_STRINGS.len())?
        .checked_add(json_string_content_encoded_len(&response.body)?)?
        .checked_add(BEFORE_STATUS.len())?
        .checked_add(decimal_digits(response.status))?
        .checked_add(1)
}

pub(crate) fn response_form_encoded_len(response: &ResourceResponse) -> Option<usize> {
    const FORM_FIELDS: usize = "status=".len() + "&body=".len();

    FORM_FIELDS
        .checked_add(decimal_digits(response.status))?
        .checked_add(form_component_encoded_len(&response.body)?)
}

fn json_string_content_encoded_len(value: &str) -> Option<usize> {
    value.bytes().try_fold(0_usize, |length, byte| {
        let encoded = match byte {
            b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            b'\x00'..=b'\x1f' => 6,
            _ => 1,
        };
        length.checked_add(encoded)
    })
}

fn form_component_encoded_len(value: &str) -> Option<usize> {
    value.bytes().try_fold(0_usize, |length, byte| {
        let encoded =
            if byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_' | b' ') {
                1
            } else {
                3
            };
        length.checked_add(encoded)
    })
}

fn decimal_digits(value: u16) -> usize {
    match value {
        0..=9 => 1,
        10..=99 => 2,
        100..=999 => 3,
        1000..=9999 => 4,
        _ => 5,
    }
}

pub const PING_JSON: &str = r#"{"type":"ping"}"#;
pub const PONG_JSON: &str = r#"{"type":"pong"}"#;

#[cfg(test)]
mod tests {
    use super::{
        ready_json, register_json, response_form_encoded_len, response_json,
        response_json_encoded_len, validate_instance, validate_worker, IncomingMessage,
        ResourceResponse, MAX_INSTANCE_SIZE, MAX_RESOURCE_SIZE, MAX_UID_SIZE, MAX_WORKER_SIZE,
    };
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

    #[test]
    fn protocol_identifier_caps_are_byte_based_and_include_boundaries() {
        let ready = IncomingMessage::Ready {
            uid: "u".repeat(MAX_UID_SIZE),
        };
        assert!(ready.validate().is_ok());
        let ready = IncomingMessage::Ready {
            uid: "u".repeat(MAX_UID_SIZE + 1),
        };
        assert!(ready.validate().is_err());

        let request = IncomingMessage::Request {
            uid: "u".into(),
            resource: "r".repeat(MAX_RESOURCE_SIZE),
        };
        assert!(request.validate().is_ok());
        let request = IncomingMessage::Request {
            uid: "u".into(),
            resource: "r".repeat(MAX_RESOURCE_SIZE + 1),
        };
        assert!(request.validate().is_err());

        let multibyte_uid = IncomingMessage::Ready {
            uid: "é".repeat(MAX_UID_SIZE / 2 + 1),
        };
        assert!(multibyte_uid.validate().is_err());
        assert!(validate_instance(&"i".repeat(MAX_INSTANCE_SIZE)).is_ok());
        assert!(validate_instance(&"i".repeat(MAX_INSTANCE_SIZE + 1)).is_err());
        assert!(validate_worker(&"w".repeat(MAX_WORKER_SIZE)).is_ok());
        assert!(validate_worker(&"w".repeat(MAX_WORKER_SIZE + 1)).is_err());
    }

    #[test]
    fn allocation_free_size_prechecks_match_wire_encoders() {
        let mut body = String::from_utf8((0_u8..=127).collect()).unwrap();
        body.push_str("é🙂");
        let response = ResourceResponse::new("uid/with space".into(), 201, body);

        assert_eq!(
            response_json_encoded_len(&response).unwrap(),
            response_json(&response).unwrap().len()
        );

        crate::install_rustls_provider();
        let status = response.status.to_string();
        let request = reqwest::Client::new()
            .post("http://example.test/")
            .form(&[
                ("status", status.as_str()),
                ("body", response.body.as_str()),
            ])
            .build()
            .unwrap();
        let encoded = request.body().unwrap().as_bytes().unwrap();
        assert_eq!(response_form_encoded_len(&response).unwrap(), encoded.len());
    }
}
