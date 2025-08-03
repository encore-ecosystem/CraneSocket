use log::{error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, broadcast};

use crate::server::common::Listener;
use crate::server::error::ProxyServerError;
use crate::server::message_handler::handle_message;
use crate::server::transport::{TransportMessage, WebSocketTransport};
use crate::socket::proxy::WebSocketListener;
use crate::socket::upnp::WebSocketListener as UPnPWebSocketListener;

pub mod common;
pub mod constant;
pub mod error;
pub mod message;
mod message_handler;
mod transport;
mod utils;

pub type Sender = tokio::sync::mpsc::Sender<TransportMessage>;
pub type Room = HashMap<String, Sender>;
pub type Rooms = Arc<RwLock<HashMap<String, Room>>>;
pub type Peer2Room = Arc<RwLock<HashMap<String, String>>>;

pub struct ProxyServer {
    rooms: Rooms,
    peer2room: Peer2Room,
}

impl Default for ProxyServer {
    fn default() -> Self {
        ProxyServer::new()
    }
}

impl ProxyServer {
    pub fn new() -> Self {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let peer2room: Peer2Room = Arc::new(RwLock::new(HashMap::new()));
        ProxyServer { rooms, peer2room }
    }

    pub async fn serve(
        self,
        addr: &SocketAddr,
        use_upnp: bool,
        mut shutdown_rx: Receiver<()>,
    ) -> Result<(), ProxyServerError> {
        let listener = match use_upnp {
            true => Listener::UPnPWebSocketListener(UPnPWebSocketListener::listen(addr).await?),
            false => Listener::WebSocketListener(WebSocketListener::listen(addr).await?),
        };
        info!("Proxy server started listening on {}", addr);

        loop {
            tokio::select! {
                _ = async {
                    loop {
                        let (stream, addr) = listener.accept().await.unwrap();
                        info!("New WebSocket connection: {}", addr);
                        let rooms = self.rooms.clone();
                        let peer2room = self.peer2room.clone();
                        tokio::spawn(async move {
                            let peer_id = addr.to_string();
                            let (shutdown_tx, _) = broadcast::channel(1);
                            if let Ok(transport) = WebSocketTransport::new(stream, peer_id.clone(), shutdown_tx.clone()).await {
                                if let Err(e) = handle_message(transport, rooms, peer2room, shutdown_tx.clone()).await {
                                    error!("WebSocket connection error {}: {}", addr, e);
                                }
                            }
                        });
                    }
                } => {},
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received. Shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}
