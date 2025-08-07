use async_trait::async_trait;
use tokio::sync::mpsc::{self, error::TryRecvError};
pub use ws::WebSocketTransport;

use crate::server::{
    error::ProxyServerError,
    message::{ClientMessage, ServerMessage},
};

mod tcp;
mod udp;
mod ws;

#[allow(dead_code)]
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, message: ServerMessage) -> Result<(), ProxyServerError>;
    async fn recv(&mut self) -> Option<Result<ClientMessage, ProxyServerError>>;
    fn try_recv(&mut self) -> Result<ClientMessage, TryRecvError>;
    fn peer_id(&self) -> &str;
    fn sender(&self) -> mpsc::Sender<ServerMessage>;
}
