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
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Socket Error")]
    Socket,
    #[error("Unexpected Message error: {0}")]
    UnexpectedMessage(String),
    #[error("Room is unavailable")]
    RoomUnavailable,
    #[error("Timeout")]
    Timeout,
    #[error("UnexpectedClose")]
    UnexpectedClose,
}
