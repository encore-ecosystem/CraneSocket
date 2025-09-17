use log::{error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, broadcast};

use crate::server::common::close_server;
use crate::server::constant::SHUTDOWN_CHANNEL_CAPACITY;
use crate::server::error::ProxyServerError;
use crate::server::message::ServerMessage;
use crate::server::message_handler::handle_message;
use crate::server::transport::WebSocketTransport;
use crate::server::transport::{TcpTransport, UdpTransport};
use crate::socket::proxy::TcpListener as ProxyTcpListener;
use crate::socket::proxy::UdpListener as ProxyUdpListener;
use crate::socket::proxy::WebSocketListener as ProxyWebSocketListener;
use crate::socket::upnp::UPnPManager;
use crate::socket::utils::ConnectionProtocol;

pub mod common;
pub mod constant;
pub mod error;
pub mod message;
mod message_handler;
mod transport;
mod utils;

pub type Sender = tokio::sync::mpsc::Sender<(ServerMessage, SocketAddr)>;
pub type Room = HashMap<SocketAddr, Sender>;
pub type Rooms = Arc<RwLock<HashMap<String, Room>>>;
pub type Client2Room = Arc<RwLock<HashMap<SocketAddr, String>>>;

#[allow(dead_code)]
pub struct WebSocketProxyServer {
    listener: ProxyWebSocketListener,
    upnp_manager: Option<UPnPManager>,
    rooms: Rooms,
    client2room: Client2Room,
}

impl WebSocketProxyServer {
    pub async fn bind(addr: &SocketAddr, use_upnp: bool) -> Result<Self, ProxyServerError> {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let client2room: Client2Room = Arc::new(RwLock::new(HashMap::new()));
        let listener = ProxyWebSocketListener::listen(addr).await?;

        let upnp_manager = match use_upnp {
            true => Some(
                UPnPManager::new(addr.port(), ConnectionProtocol::WebSocket).map_err(|e| {
                    ProxyServerError::Listener(crate::socket::ListenerError::Upnp(e))
                })?,
            ),
            false => None,
        };

        Ok(Self {
            listener,
            upnp_manager,
            rooms,
            client2room,
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
                        let client2room = self.client2room.clone();
                        tokio::spawn(async move {
                            let (shutdown_tx, _) = broadcast::channel(SHUTDOWN_CHANNEL_CAPACITY);
                            if let Ok(transport) = WebSocketTransport::new(stream, addr, shutdown_tx.clone()).await {
                                if let Err(e) = handle_message(transport, rooms, client2room, shutdown_tx.clone()).await {
                                    error!("WebSocket connection error {}: {}", addr, e);
                                }
                            }
                        });
                    }
                } => {},
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received. Shutting down...");
                    close_server(self.rooms.clone()).await;
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

    pub fn get_client2room(&self) -> Client2Room {
        self.client2room.clone()
    }
}

#[allow(dead_code)]
pub struct TcpProxyServer {
    listener: ProxyTcpListener,
    upnp_manager: Option<UPnPManager>,
    rooms: Rooms,
    client2room: Client2Room,
}

impl TcpProxyServer {
    pub async fn bind(addr: &SocketAddr, use_upnp: bool) -> Result<Self, ProxyServerError> {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let client2room: Client2Room = Arc::new(RwLock::new(HashMap::new()));
        let listener = ProxyTcpListener::listen(addr).await?;
        let upnp_manager = match use_upnp {
            true => Some(
                UPnPManager::new(addr.port(), ConnectionProtocol::Tcp).map_err(|e| {
                    ProxyServerError::Listener(crate::socket::ListenerError::Upnp(e))
                })?,
            ),
            false => None,
        };

        Ok(Self {
            listener,
            upnp_manager,
            rooms,
            client2room,
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
                        let client2room = self.client2room.clone();
                        tokio::spawn(async move {
                            let (shutdown_tx, _) = broadcast::channel(SHUTDOWN_CHANNEL_CAPACITY);
                            if let Ok(transport) = TcpTransport::new(stream, addr, shutdown_tx.clone()).await {
                                if let Err(e) = handle_message(transport, rooms, client2room, shutdown_tx.clone()).await {
                                    error!("TCP connection error {}: {}", addr, e);
                                }
                            }
                        });
                    }
                } => {},
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received. Shutting down...");
                    close_server(self.rooms.clone()).await;
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

    pub fn get_client2room(&self) -> Client2Room {
        self.client2room.clone()
    }
}

#[allow(dead_code)]
pub struct UdpProxyServer {
    socket: Arc<ProxyUdpListener>,
    upnp_manager: Option<UPnPManager>,
    rooms: Rooms,
    client2room: Client2Room,
    error_counter_per_client: Arc<RwLock<HashMap<SocketAddr, usize>>>,
}

impl UdpProxyServer {
    pub async fn bind(addr: &SocketAddr, use_upnp: bool) -> Result<Self, ProxyServerError> {
        let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));
        let client2room: Client2Room = Arc::new(RwLock::new(HashMap::new()));
        let error_counter_per_client = Arc::new(RwLock::new(HashMap::new()));
        let socket = ProxyUdpListener::bind(addr).await?;
        let upnp_manager = match use_upnp {
            true => Some(
                UPnPManager::new(addr.port(), ConnectionProtocol::Udp).map_err(|e| {
                    ProxyServerError::Listener(crate::socket::ListenerError::Upnp(e))
                })?,
            ),
            false => None,
        };
        Ok(Self {
            socket: Arc::new(socket),
            upnp_manager,
            rooms,
            client2room,
            error_counter_per_client,
        })
    }

    pub async fn serve(&self, shutdown_rx: Receiver<()>) -> Result<(), ProxyServerError> {
        info!(
            "Proxy server started serving on {}",
            self.socket.get_local_addr()?
        );

        let rooms_clone = self.rooms.clone();
        let client2room_clone = self.client2room.clone();
        let error_counter_per_client_clone = self.error_counter_per_client.clone();
        let socket_clone = self.socket.clone();

        let (shutdown_tx, _) = broadcast::channel(SHUTDOWN_CHANNEL_CAPACITY);
        let transport = UdpTransport::new(
            socket_clone,
            shutdown_tx.clone(),
            error_counter_per_client_clone,
        )
        .await?;
        let actors_handle = tokio::spawn(async move {
            {
                if let Err(e) = handle_message(
                    transport,
                    rooms_clone,
                    client2room_clone,
                    shutdown_tx.clone(),
                )
                .await
                {
                    error!("UDP connection error: {}", e);
                }
            }
        });
        shutdown_rx.await.unwrap();
        info!("Shutdown signal received. Shutting down...");
        close_server(self.rooms.clone()).await;
        actors_handle.abort();

        Ok(())
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ProxyServerError> {
        self.socket
            .get_local_addr()
            .map_err(ProxyServerError::Listener)
    }

    pub fn get_rooms(&self) -> Rooms {
        self.rooms.clone()
    }

    pub fn get_client2room(&self) -> Client2Room {
        self.client2room.clone()
    }

    pub async fn get_error_counter_per_client(&self) -> Arc<RwLock<HashMap<SocketAddr, usize>>> {
        self.error_counter_per_client.clone()
    }
}
