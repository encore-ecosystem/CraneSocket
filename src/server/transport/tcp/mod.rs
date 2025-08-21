use std::net::SocketAddr;

use async_trait::async_trait;
use log::debug;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::server::constant::MPSC_CHANNEL_CAPACITY;
use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::transport::Transport;
use crate::socket::proxy::ReadHalf;
use crate::socket::proxy::WriteHalf;

mod actors;

use actors::*;

pub struct TcpTransport {
    addr: SocketAddr,
    send_tx: mpsc::Sender<(ServerMessage, SocketAddr)>,
    recv_rx: mpsc::Receiver<(ClientMessage, SocketAddr)>,
}

impl TcpTransport {
    pub async fn new(
        stream: TcpStream,
        addr: SocketAddr,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<Self, ProxyServerError> {
        let (receiver, sender) = stream.into_split();
        let receiver = ReadHalf::new(receiver);
        let sender = WriteHalf::new(sender);

        let (send_tx, send_rx) =
            mpsc::channel::<(ServerMessage, SocketAddr)>(MPSC_CHANNEL_CAPACITY);
        let (recv_tx, recv_rx) =
            mpsc::channel::<(ClientMessage, SocketAddr)>(MPSC_CHANNEL_CAPACITY);

        let addr_clone = addr;
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            app2socket_actor(
                addr_clone,
                sender,
                send_rx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });
        debug!("app2socket actor started");

        let addr_clone = addr;
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            socket2app_actor(
                addr_clone,
                receiver,
                recv_tx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });
        debug!("socket2app actor started");

        Ok(TcpTransport {
            addr,
            send_tx,
            recv_rx,
        })
    }
}

#[async_trait]
impl Transport for TcpTransport {
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
        Some(self.addr)
    }

    fn get_sender(&self) -> mpsc::Sender<(ServerMessage, SocketAddr)> {
        self.send_tx.clone()
    }
}
