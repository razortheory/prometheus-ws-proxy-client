pub mod cli;
pub mod config;
pub mod protocol;
pub mod resource;
pub mod target;
pub mod worker;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub const MAX_BODY_SIZE: usize = 64 * 1024 * 1024;
pub const MAX_WS_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
