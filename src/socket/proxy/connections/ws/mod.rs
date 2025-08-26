use crate::{
    server::message::{ClientMessage, ServerMessage},
    socket::{
        ConnectionError,
        config::{ConnectionConfig, ConnectionMethod},
        utils::ConnectionProtocol,
    },
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
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
}

impl WebSocketConnection {
    pub fn new(
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        server_addr: Option<SocketAddr>,
        room_id: Option<String>,
    ) -> Self {
        Self {
            stream,
            server_addr,
            room_id,
        }
    }

    pub async fn from_token(token: &str) -> Result<Self, ConnectionError> {
        let cfg = ConnectionConfig::decode(token).map_err(ConnectionError::Serialization)?;

        if cfg.protocol != ConnectionProtocol::WebSocket || cfg.method != ConnectionMethod::Proxy {
            return Err(ConnectionError::InvalidConfig);
        }

        let room_id = cfg.room_id.ok_or(ConnectionError::InvalidConfig)?;
        Self::join_room(&cfg.addr, room_id).await
    }

    pub async fn create_room(
        server_addr: &SocketAddr,
    ) -> Result<(WebSocketConnection, String), ConnectionError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", server_addr)).await?;
        debug!("Connected to the proxy server");

        let room_id = register(&mut server_conn).await?;
        debug!("Created a room on the proxy server. room_id={}", room_id);

        Ok((
            WebSocketConnection::new(server_conn, Some(*server_addr), Some(room_id.clone())),
            room_id,
        ))
    }

    pub async fn join_room(addr: &SocketAddr, room_id: String) -> Result<Self, ConnectionError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", addr)).await?;
        debug!("Connected to the proxy server");

        join_room(&mut server_conn, room_id.clone()).await?;
        debug!("Joined the room on the proxy server");

        let conn = Self::new(server_conn, Some(*addr), Some(room_id));
        Ok(conn)
    }

    pub async fn wait_for_client(&mut self) -> Result<(), ConnectionError> {
        wait_for_another_client(&mut self.stream).await?;
        debug!("Another peer successfully connected to proxy server");
        Ok(())
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
        let server_addr = self.server_addr;
        let room_id = self.room_id;
        (
            WebSocketSender {
                sink,
                server_addr,
                room_id: room_id.clone(),
            },
            WebSocketReceiver {
                stream,
                server_addr,
                room_id,
            },
        )
    }

    pub fn reunite(
        sender: WebSocketSender,
        receiver: WebSocketReceiver,
    ) -> Result<Self, ConnectionError> {
        let stream = SplitSink::reunite(sender.sink, receiver.stream)
            .map_err(|_| ConnectionError::Socket)?;
        if sender.room_id != receiver.room_id || sender.server_addr != receiver.server_addr {
            return Err(ConnectionError::Socket);
        }
        let server_addr = sender.server_addr;
        let room_id = sender.room_id;
        Ok(Self {
            stream,
            server_addr,
            room_id,
        })
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ConnectionError> {
        let raw: &MaybeTlsStream<TcpStream> = self.stream.get_ref();
        match raw {
            MaybeTlsStream::Plain(stream) => Ok(stream.local_addr()?),
            _ => Err(ConnectionError::Io(std::io::Error::other(
                "Unsupported stream type",
            ))),
        }
    }

    pub fn get_peer_addr(&self) -> Result<SocketAddr, ConnectionError> {
        let raw: &MaybeTlsStream<TcpStream> = self.stream.get_ref();
        match raw {
            MaybeTlsStream::Plain(stream) => Ok(stream.peer_addr()?),
            _ => Err(ConnectionError::Io(std::io::Error::other(
                "Unsupported stream type",
            ))),
        }
    }

    pub fn get_token(&self) -> Result<String, ConnectionError> {
        let server_addr = self.server_addr.ok_or(ConnectionError::NotConnected)?;
        let room_id = self.room_id.clone().ok_or(ConnectionError::NotConnected)?;

        let cfg = ConnectionConfig::new(
            ConnectionMethod::Proxy,
            ConnectionProtocol::WebSocket,
            server_addr,
            Some(room_id),
        );

        cfg.encode().map_err(ConnectionError::Serialization)
    }
}

pub struct WebSocketSender {
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
}

impl WebSocketSender {
    pub async fn send(&mut self, msg: ClientMessage) -> Result<(), ConnectionError> {
        Ok(self.sink.send(msg.into()).await?)
    }
}

pub struct WebSocketReceiver {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
}

impl WebSocketReceiver {
    pub async fn next(&mut self) -> Result<ServerMessage, ConnectionError> {
        match self.stream.next().await.unwrap() {
            Ok(msg) => Ok(msg.into()),
            Err(e) => Err(ConnectionError::WebSocket(e)),
        }
    }
}
