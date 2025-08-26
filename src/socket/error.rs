use std::{io, net::SocketAddr};
use stunclient::Error as StunClientError;
use thiserror::Error;

use crate::{server::message::MessageError, socket::upnp::UPnPManagerError};

#[derive(Error, Debug)]
pub enum StunError {
    #[error("Stun error: {0}")]
    Upnp(#[from] StunClientError),
}

#[derive(Error, Debug)]
pub enum ListenerError {
    #[error("Socket error")]
    Socket,
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Unexpected Message error: {0}")]
    UnexpectedMessage(String),
    #[error("Invalid Datagram error: {0}")]
    InvalidDatagramFrom(SocketAddr),
    #[error("Serialization Error: {0}")]
    Serialization(postcard::Error),
    #[error("InvalidConfig")]
    InvalidConfig,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("UPnP error: {0}")]
    Upnp(#[from] UPnPManagerError),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Socket Error")]
    Socket,
    #[error("Unexpected Message error: {0}")]
    UnexpectedMessage(String),
    #[error("Unexpected Message error: {0}")]
    InvalidMessage(#[from] MessageError),
    #[error("Room is unavailable")]
    RoomUnavailable,
    #[error("Timeout")]
    Timeout,
    #[error("UnexpectedClose")]
    UnexpectedClose,
    #[error("NotConnected")]
    NotConnected,
    #[error("Serialization Error: {0}")]
    Serialization(postcard::Error),
    #[error("InvalidConfig")]
    InvalidConfig,
}
