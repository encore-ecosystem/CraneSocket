use async_trait::async_trait;
use log::debug;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::transport::Transport;
use crate::socket::proxy::ReadHalf;
use crate::socket::proxy::WriteHalf;

mod actors;

use actors::*;

pub struct TcpTransport {
    peer_id: String,
    send_tx: mpsc::Sender<ServerMessage>,
    recv_rx: mpsc::Receiver<ClientMessage>,
}

impl TcpTransport {
    pub async fn new(
        stream: TcpStream,
        peer_id: String,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<Self, ProxyServerError> {
        let (receiver, sender) = stream.into_split();
        let receiver = ReadHalf::new(receiver);
        let sender = WriteHalf::new(sender);

        let (send_tx, send_rx) = mpsc::channel::<ServerMessage>(100);
        let (recv_tx, recv_rx) = mpsc::channel::<ClientMessage>(100);

        let peer_id_clone = peer_id.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            app2socket_actor(
                peer_id_clone,
                sender,
                send_rx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });
        debug!("app2socket actor started");

        let peer_id_clone = peer_id.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            socket2app_actor(
                peer_id_clone,
                receiver,
                recv_tx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });
        debug!("socket2app actor started");

        Ok(TcpTransport {
            peer_id,
            send_tx,
            recv_rx,
        })
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, message: ServerMessage) -> Result<(), ProxyServerError> {
        self.send_tx
            .send(message)
            .await
            .map_err(|e| ProxyServerError::Send(e.to_string()))?;
        Ok(())
    }

    async fn recv(&mut self) -> Option<Result<ClientMessage, ProxyServerError>> {
        self.recv_rx.recv().await.map(Ok)
    }

    fn try_recv(&mut self) -> Result<ClientMessage, TryRecvError> {
        self.recv_rx.try_recv()
    }

    fn peer_id(&self) -> &str {
        &self.peer_id
    }
    fn sender(&self) -> mpsc::Sender<ServerMessage> {
        self.send_tx.clone()
    }
}
