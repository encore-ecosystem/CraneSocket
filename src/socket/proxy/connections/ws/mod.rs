use crate::{
    server::message::{ClientMessage, ServerMessage},
    socket::ConnectionError,
};
use futures::{
    SinkExt,
    stream::{SplitSink, SplitStream},
};
use futures_util::StreamExt;
use log::debug;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message as TungsteniteMessage, protocol::CloseFrame},
};

mod connection_logic;

use connection_logic::*;

#[derive(Debug)]
pub struct WebSocketConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WebSocketConnection {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { stream }
    }

    pub async fn create_room(
        server_addr: &SocketAddr,
    ) -> Result<(WebSocketConnection, String), ConnectionError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", server_addr)).await?;
        debug!("Connected to proxy server");

        let room_id = register(&mut server_conn).await?;
        debug!("Created a room on proxy server");

        Ok((WebSocketConnection::new(server_conn), room_id))
    }

    pub async fn wait_for_client(&mut self) -> Result<(), ConnectionError> {
        wait_for_another_peer(&mut self.stream).await?;
        debug!("Another peer successfully connected to proxy server");
        Ok(())
    }

    pub async fn join_room(addr: &SocketAddr, room_id: String) -> Result<Self, ConnectionError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", addr)).await?;

        join_room(&mut server_conn, room_id).await?;

        let conn = Self::new(server_conn);
        Ok(conn)
    }

    pub async fn send(&mut self, msg: ClientMessage) -> Result<(), ConnectionError> {
        Ok(self.stream.send(msg.into()).await?)
    }

    pub async fn next(&mut self) -> Result<ServerMessage, ConnectionError> {
        match self.stream.next().await.unwrap() {
            Ok(msg) => Ok(msg.into()),
            Err(e) => Err(ConnectionError::WebSocket(e)),
        }
    }

    pub async fn close(&mut self, frame: Option<CloseFrame>) -> Result<(), ConnectionError> {
        Ok(self.stream.close(frame).await?)
    }

    pub fn split(self) -> (WebSocketSender, WebSocketReceiver) {
        let (sink, stream) = self.stream.split();
        (WebSocketSender { sink }, WebSocketReceiver { stream })
    }

    pub fn reunite(
        sender: WebSocketSender,
        receiver: WebSocketReceiver,
    ) -> Result<Self, ConnectionError> {
        let stream =
            SplitSink::reunite(sender.sink, receiver.stream).map_err(|_| ConnectionError::Io)?;
        Ok(Self { stream })
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ConnectionError> {
        let raw: &MaybeTlsStream<TcpStream> = self.stream.get_ref();
        match raw {
            MaybeTlsStream::Plain(stream) => Ok(stream.local_addr()?),
            _ => Err(ConnectionError::Socket(std::io::Error::other(
                "Unsupported stream type",
            ))),
        }
    }

    pub fn get_peer_addr(&self) -> Result<SocketAddr, ConnectionError> {
        let raw: &MaybeTlsStream<TcpStream> = self.stream.get_ref();
        match raw {
            MaybeTlsStream::Plain(stream) => Ok(stream.peer_addr()?),
            _ => Err(ConnectionError::Socket(std::io::Error::other(
                "Unsupported stream type",
            ))),
        }
    }
}

pub struct WebSocketSender {
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>,
}

impl WebSocketSender {
    pub async fn send(&mut self, msg: ClientMessage) -> Result<(), ConnectionError> {
        Ok(self.sink.send(msg.into()).await?)
    }
}

pub struct WebSocketReceiver {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl WebSocketReceiver {
    pub async fn next(&mut self) -> Result<ServerMessage, ConnectionError> {
        match self.stream.next().await.unwrap() {
            Ok(msg) => Ok(msg.into()),
            Err(e) => Err(ConnectionError::WebSocket(e)),
        }
    }
}
