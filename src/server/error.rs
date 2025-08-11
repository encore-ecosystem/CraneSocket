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
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Send Error: {0}")]
    Send(String),
    #[error("Listener error: {0}")]
    Listener(#[from] ListenerError),
    #[error("Invalid data: {0}")]
    InvalidData(String),
}
