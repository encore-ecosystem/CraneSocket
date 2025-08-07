use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::transport::Transport;

mod actors;
mod utils;

use actors::*;

pub struct WebSocketTransport {
    peer_id: String,
    send_tx: mpsc::Sender<ServerMessage>,
    recv_rx: mpsc::Receiver<ClientMessage>,
}

impl WebSocketTransport {
    pub async fn new(
        ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        peer_id: String,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<Self, ProxyServerError> {
        let (ws_sender, ws_receiver) = ws_stream.split();

        let (send_tx, send_rx) = mpsc::channel::<ServerMessage>(100);
        let (recv_tx, recv_rx) = mpsc::channel::<ClientMessage>(100);

        let peer_id_clone = peer_id.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            app2socket_actor(
                peer_id_clone,
                ws_sender,
                send_rx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });

        let peer_id_clone = peer_id.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_tx_sub = shutdown_tx.subscribe();
        tokio::spawn(async move {
            socket2app_actor(
                peer_id_clone,
                ws_receiver,
                recv_tx,
                shutdown_tx_clone,
                shutdown_tx_sub,
            )
            .await
        });

        Ok(WebSocketTransport {
            peer_id,
            send_tx,
            recv_rx,
        })
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
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
