use stunclient::Error as StunClientError;
use thiserror::Error;

use crate::socket::ListenerError;

#[derive(Error, Debug)]
pub enum StunError {
    #[error("Stun error: {0}")]
    Upnp(#[from] StunClientError),
}

#[derive(Error, Debug)]
pub enum ProxyServerError {
    #[error("Socket error: {0}")]
    Socket(String),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Send Error: {0}")]
    Send(String),
    #[error("SerializationError: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Operation not supported for this connection type")]
    UnsupportedOperation,
    #[error("IO Error")]
    Io(#[from] std::io::Error),
    #[error("Listener error: {0}")]
    Listener(#[from] ListenerError),
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Not Found error: {0}")]
    NotFound(String),
}
