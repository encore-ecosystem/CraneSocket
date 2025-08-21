use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::server::constant::MPSC_CHANNEL_CAPACITY;
use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::transport::Transport;
use crate::socket::proxy::UdpListener;

mod actors;
use actors::*;

pub struct UdpTransport {
    send_tx: mpsc::Sender<(ServerMessage, SocketAddr)>,
    recv_rx: mpsc::Receiver<(ClientMessage, SocketAddr)>,
}

impl UdpTransport {
    pub async fn new(
        socket: Arc<UdpListener>,
        shutdown_tx: broadcast::Sender<()>,
        error_counter_per_client: Arc<RwLock<HashMap<SocketAddr, usize>>>,
    ) -> Result<Self, ProxyServerError> {
        let (send_tx, send_rx) =
            mpsc::channel::<(ServerMessage, SocketAddr)>(MPSC_CHANNEL_CAPACITY);
        let (recv_tx, recv_rx) =
            mpsc::channel::<(ClientMessage, SocketAddr)>(MPSC_CHANNEL_CAPACITY);

        let socket_clone = socket.clone();
        let error_counter_per_client_clone = error_counter_per_client.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            app2socket_actor(
                socket_clone,
                send_rx,
                shutdown_tx_sub,
                error_counter_per_client_clone,
            )
            .await
        });
        debug!("app2socket actor started");

        let socket_clone = socket.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        let error_counter_per_client_clone = error_counter_per_client.clone();
        tokio::spawn(async move {
            socket2app_actor(
                socket_clone,
                recv_tx,
                shutdown_tx_sub,
                error_counter_per_client_clone,
            )
            .await
        });
        debug!("socket2app actor started");

        Ok(UdpTransport { send_tx, recv_rx })
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn send(&self, msg: ServerMessage, addr: SocketAddr) -> Result<(), ProxyServerError> {
        self.send_tx
            .send((msg, addr))
            .await
            .map_err(|e| ProxyServerError::Send(e.to_string()))?;
        Ok(())
    }

    async fn recv(&mut self) -> Option<Result<(ClientMessage, SocketAddr), ProxyServerError>> {
        self.recv_rx.recv().await.map(Ok)
    }

    fn try_recv(&mut self) -> Result<(ClientMessage, SocketAddr), TryRecvError> {
        self.recv_rx.try_recv()
    }

    fn get_client_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn get_sender(&self) -> mpsc::Sender<(ServerMessage, SocketAddr)> {
        self.send_tx.clone()
    }
}
