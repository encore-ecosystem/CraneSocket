use thiserror::Error;

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("InvalidState")]
    InvalidState,
    #[error("FailedHandShake")]
    FailedHandShake,
    #[error("Write")]
    Write,
    #[error("Read")]
    Read,
    #[error("Decrypt")]
    Decrypt,
    #[error("Encrypt")]
    Encrypt,
    #[error("Send")]
    Send,
    #[error("Timeout")]
    Timeout,
    #[error("InvalidMessageType")]
    InvalidMessageType,
}
