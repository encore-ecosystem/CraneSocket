use crate::{
    server::message::{ClientMessage, ServerMessage},
    socket::{
        ConnectionError,
        config::{ConnectionConfig, ConnectionMethod},
        crypto::{CryptoError, constant::MAX_BUFFER_SIZE},
        proxy::ws::{
            connection_logic::{join_room, register, wait_for_another_client},
            crypto::{decrypt_message, encrypt_message, establish_encryption},
        },
        utils::ConnectionProtocol,
    },
};
use futures::{
    SinkExt,
    stream::{SplitSink, SplitStream},
};
use futures_util::StreamExt;
use log::debug;
use snow::TransportState;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as TungsteniteMessage,
};

#[derive(Debug)]
pub struct WebSocketConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    noise: Option<TransportState>,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
    buffer: Vec<u8>,
}

impl WebSocketConnection {
    pub fn new(
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        noise: Option<TransportState>,
        server_addr: Option<SocketAddr>,
        room_id: Option<String>,
    ) -> Self {
        Self {
            stream,
            noise,
            server_addr,
            room_id,
            buffer: vec![0u8; MAX_BUFFER_SIZE],
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
        let (mut stream, _response) = connect_async(format!("ws://{}", server_addr)).await?;
        debug!("Connected to the proxy server");

        let room_id = register(&mut stream).await?;
        debug!("Created a room on the proxy server. room_id={}", room_id);

        Ok((
            WebSocketConnection::new(stream, None, Some(*server_addr), Some(room_id.clone())),
            room_id,
        ))
    }

    pub async fn join_room(addr: &SocketAddr, room_id: String) -> Result<Self, ConnectionError> {
        let (mut stream, _response) = connect_async(format!("ws://{}", addr)).await?;
        debug!("Connected to the proxy server");

        join_room(&mut stream, room_id.clone()).await?;
        debug!("Joined the room on the proxy server");

        let noise = establish_encryption(&mut stream, false).await?;
        debug!("Successfully established encryption with another clinet");

        let conn = Self::new(stream, Some(noise), Some(*addr), Some(room_id));
        Ok(conn)
    }

    pub async fn wait_for_client(&mut self) -> Result<(), ConnectionError> {
        wait_for_another_client(&mut self.stream).await?;
        debug!("Another peer successfully connected to proxy server");

        let noise = establish_encryption(&mut self.stream, true).await?;
        self.noise = Some(noise);
        debug!("Successfully established encryption with another clinet");

        Ok(())
    }

    pub async fn send(&mut self, msg: ClientMessage) -> Result<(), ConnectionError> {
        let msg = match &mut self.noise {
            Some(noise) => encrypt_message(msg, noise, &mut self.buffer)?,
            None => msg,
        };
        Ok(self.stream.send(msg.into()).await?)
    }

    pub async fn next(&mut self) -> Result<ServerMessage, ConnectionError> {
        let msg = self
            .stream
            .next()
            .await
            .ok_or(ConnectionError::UnexpectedClose)??;
        match &mut self.noise {
            Some(noise) => decrypt_message(msg.into(), noise, &mut self.buffer)
                .map_err(ConnectionError::Crypto),
            None => Ok(msg.into()),
        }
    }

    pub async fn close(&mut self) -> Result<(), ConnectionError> {
        Ok(self.stream.close(None).await?)
    }

    pub fn split(self) -> (WebSocketSender, WebSocketReceiver) {
        let (sink, stream) = self.stream.split();
        let noise = Arc::new(Mutex::new(self.noise));
        let server_addr = self.server_addr;
        let room_id = self.room_id;

        (
            WebSocketSender {
                sink,
                noise: noise.clone(),
                server_addr,
                room_id: room_id.clone(),
                buffer: vec![0u8; MAX_BUFFER_SIZE],
            },
            WebSocketReceiver {
                stream,
                noise,
                server_addr,
                room_id,
                buffer: vec![0u8; MAX_BUFFER_SIZE],
            },
        )
    }

    pub fn reunite(
        sender: WebSocketSender,
        receiver: WebSocketReceiver,
    ) -> Result<Self, ConnectionError> {
        if !Arc::ptr_eq(&sender.noise, &receiver.noise) {
            return Err(ConnectionError::Crypto(
                crate::socket::crypto::CryptoError::InvalidState,
            ));
        }

        let stream = SplitSink::reunite(sender.sink, receiver.stream)
            .map_err(|_| ConnectionError::Socket)?;
        if sender.room_id != receiver.room_id || sender.server_addr != receiver.server_addr {
            return Err(ConnectionError::Socket);
        }
        let noise = Arc::try_unwrap(sender.noise)
            .map_err(|_| ConnectionError::Crypto(CryptoError::InvalidState))?
            .into_inner()
            .map_err(|_| ConnectionError::Crypto(CryptoError::InvalidState))?;
        let server_addr = sender.server_addr;
        let room_id = sender.room_id;

        Ok(Self {
            stream,
            noise,
            server_addr,
            room_id,
            buffer: vec![0u8; MAX_BUFFER_SIZE],
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
    noise: Arc<Mutex<Option<TransportState>>>,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
    buffer: Vec<u8>,
}

impl WebSocketSender {
    pub async fn send(&mut self, msg: ClientMessage) -> Result<(), ConnectionError> {
        let msg = match &mut *self.noise.lock().unwrap() {
            Some(noise) => encrypt_message(msg, noise, &mut self.buffer)?,
            None => msg,
        };

        Ok(self.sink.send(msg.into()).await?)
    }
}

pub struct WebSocketReceiver {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    noise: Arc<Mutex<Option<TransportState>>>,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
    buffer: Vec<u8>,
}

impl WebSocketReceiver {
    pub async fn next(&mut self) -> Result<ServerMessage, ConnectionError> {
        let msg = self
            .stream
            .next()
            .await
            .ok_or(ConnectionError::UnexpectedClose)??;
        match &mut *self.noise.lock().unwrap() {
            Some(noise) => decrypt_message(msg.into(), noise, &mut self.buffer)
                .map_err(ConnectionError::Crypto),
            None => Ok(msg.into()),
        }
    }
}
