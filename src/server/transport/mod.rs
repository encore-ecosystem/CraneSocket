use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::server::{
    error::ProxyServerError,
    message::{ClientMessage, ServerMessage},
};

mod common;
mod tcp;
mod udp;
mod ws;

pub use tcp::TcpTransport;
pub use udp::UdpTransport;
pub use ws::WebSocketTransport;

#[allow(dead_code)]
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, msg: ServerMessage, addr: SocketAddr) -> Result<(), ProxyServerError>;
    async fn recv(&mut self) -> Option<Result<(ClientMessage, SocketAddr), ProxyServerError>>;
    fn try_recv(&mut self) -> Result<(ClientMessage, SocketAddr), TryRecvError>;
    fn get_client_addr(&self) -> Option<SocketAddr>;
    fn get_sender(&self) -> mpsc::Sender<(ServerMessage, SocketAddr)>;
}
