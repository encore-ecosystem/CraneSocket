use async_trait::async_trait;
use tokio::sync::mpsc::{self, error::TryRecvError};
pub use ws::WebSocketTransport;

use crate::server::error::ProxyServerError;

mod tcp;
mod udp;
mod ws;

#[derive(Debug)]
pub enum TransportMessage {
    Text(String),
    Binary(Vec<u8>),
}

#[allow(dead_code)]
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, message: TransportMessage) -> Result<(), ProxyServerError>;
    async fn recv(&mut self) -> Option<Result<TransportMessage, ProxyServerError>>;
    fn try_recv(&mut self) -> Result<TransportMessage, TryRecvError>;
    fn peer_id(&self) -> &str;
    fn sender(&self) -> mpsc::Sender<TransportMessage>;
}
