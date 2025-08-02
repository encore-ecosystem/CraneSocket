use std::io;
use stunclient::Error as StunClientError;
use thiserror::Error;

use crate::socket::upnp::UPnPManagerError;

#[derive(Error, Debug)]
pub enum StunError {
    #[error("Stun error: {0}")]
    Upnp(#[from] StunClientError),
}

#[derive(Error, Debug)]
pub enum ListenerError {
    #[error("Socket error: {0}")]
    Socket(String),
    #[error("ExternalIp error: {0}")]
    ExternalIp(String),
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("Invalid configuration: {0}")]
    Config(String),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Serde error: {0}")]
    SerdeSerialization(#[from] serde_json::Error),
    #[error("Serialization error")]
    Serialization,
    #[error("Unexpected Message error: {0}")]
    UnexpectedMessage(String),
    #[error("Resulting connection method is not supported")]
    CouldNotConnect,
    #[error("HolePunching Error: {0}")]
    SendResultError(#[from] SendResultError),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout,
    #[error("UnexpectedClose")]
    UnexpectedClose,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Socket error: {0}")]
    Socket(#[from] std::io::Error),
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("Operation not supported for this connection type")]
    UnsupportedOperation,
    #[error("Resulting connection method is not supported")]
    UnsupportedConnectionMethod,
    #[error("Resulting connection method is not supported")]
    CoundNotConnect,
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("IO Error")]
    Io,
    #[error("Serde error: {0}")]
    SerdeSerialization(#[from] serde_json::Error),
    #[error("Serialization error")]
    Serialization,
    #[error("HolePunching Error: {0}")]
    SendResultError(#[from] SendResultError),
    #[error("Unexpected Message")]
    UnexpectedMessage,
    #[error("Room is unavailable")]
    UnavailableRoom,
    #[error("Proxy server error")]
    ProxyServer,
}

#[derive(Error, Debug)]
pub enum SendResultError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
}
