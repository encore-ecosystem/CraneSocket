use log::{error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, broadcast};

use crate::server::common::{TcpListener, WebSocketListener};
use crate::server::error::ProxyServerError;
use crate::server::message::ServerMessage;
use crate::server::message_handler::handle_message;
use crate::server::transport::TcpTransport;
use crate::server::transport::WebSocketTransport;
use crate::socket::proxy::TcpListener as ProxyTcpListener;
use crate::socket::proxy::WebSocketListener as ProxyWebSocketListener;
use crate::socket::upnp::TcpListener as UPnPTcpListener;
use crate::socket::upnp::WebSocketListener as UPnPWebSocketListener;

pub mod common;
pub mod constant;
pub mod error;
pub mod message;
mod message_handler;
mod transport;
mod utils;

pub type Sender = tokio::sync::mpsc::Sender<ServerMessage>;
pub type Room = HashMap<String, Sender>;
pub type Rooms = Arc<RwLock<HashMap<String, Room>>>;
pub type Peer2Room = Arc<RwLock<HashMap<String, String>>>;

pub struct WebSocketProxyServer {
    listener: WebSocketListener,
    rooms: Rooms,
    peer2room: Peer2Room,
}

impl WebSocketProxyServer {
    pub async fn bind(addr: &SocketAddr, use_upnp: bool) -> Result<Self, ProxyServerError> {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let peer2room: Peer2Room = Arc::new(RwLock::new(HashMap::new()));
        let listener = match use_upnp {
            true => {
                WebSocketListener::UPnPWebSocketListener(UPnPWebSocketListener::listen(addr).await?)
            }
            false => WebSocketListener::ProxyWebSocketListener(
                ProxyWebSocketListener::listen(addr).await?,
            ),
        };
        Ok(Self {
            listener,
            rooms,
            peer2room,
        })
    }

    pub async fn serve(&self, mut shutdown_rx: Receiver<()>) -> Result<(), ProxyServerError> {
        info!(
            "Proxy server started serving on {}",
            self.listener.get_local_addr()?
        );
        loop {
            tokio::select! {
                _ = async {
                    loop {
                        let (stream, addr) = self.listener.accept().await.unwrap();
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

    pub fn get_local_addr(&self) -> Result<SocketAddr, ProxyServerError> {
        self.listener
            .get_local_addr()
            .map_err(ProxyServerError::Listener)
    }

    pub fn get_rooms(&self) -> Rooms {
        self.rooms.clone()
    }

    pub fn get_peer2room(&self) -> Peer2Room {
        self.peer2room.clone()
    }
}

pub struct TcpProxyServer {
    listener: TcpListener,
    rooms: Rooms,
    peer2room: Peer2Room,
}

impl TcpProxyServer {
    pub async fn bind(addr: &SocketAddr, use_upnp: bool) -> Result<Self, ProxyServerError> {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let peer2room: Peer2Room = Arc::new(RwLock::new(HashMap::new()));
        let listener = match use_upnp {
            true => TcpListener::UPnPTcpListener(UPnPTcpListener::listen(addr).await?),
            false => TcpListener::ProxyTcpListener(ProxyTcpListener::listen(addr).await?),
        };
        Ok(Self {
            listener,
            rooms,
            peer2room,
        })
    }

    pub async fn serve(&self, mut shutdown_rx: Receiver<()>) -> Result<(), ProxyServerError> {
        info!(
            "Proxy server started serving on {}",
            self.listener.get_local_addr()?
        );
        loop {
            tokio::select! {
                _ = async {
                    loop {
                        let (stream, addr) = self.listener.accept().await.unwrap();
                        info!("New TCP connection: {}", addr);
                        let rooms = self.rooms.clone();
                        let peer2room = self.peer2room.clone();
                        tokio::spawn(async move {
                            let peer_id = addr.to_string();
                            let (shutdown_tx, _) = broadcast::channel(1);
                            if let Ok(transport) = TcpTransport::new(stream, peer_id.clone(), shutdown_tx.clone()).await {
                                if let Err(e) = handle_message(transport, rooms, peer2room, shutdown_tx.clone()).await {
                                    error!("TCP connection error {}: {}", addr, e);
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

    pub fn get_local_addr(&self) -> Result<SocketAddr, ProxyServerError> {
        self.listener
            .get_local_addr()
            .map_err(ProxyServerError::Listener)
    }

    pub fn get_rooms(&self) -> Rooms {
        self.rooms.clone()
    }

    pub fn get_peer2room(&self) -> Peer2Room {
        self.peer2room.clone()
    }
}
