use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Base64 decoder error: {0}")]
    Decoder(#[from] base64::DecodeError),
}
