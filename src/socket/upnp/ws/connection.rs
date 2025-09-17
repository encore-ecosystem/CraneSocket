use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::debug;
use snow::TransportState;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

use crate::socket::{
    ConnectionError,
    config::{ConnectionConfig, ConnectionMethod},
    crypto::{CryptoError, constant::MAX_BUFFER_SIZE},
    upnp::ws::crypto::{decrypt_message, encrypt_message, establish_encryption},
    utils::ConnectionProtocol,
};

pub struct WebSocketConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    noise: TransportState,
    buffer: Vec<u8>,
}

impl WebSocketConnection {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>, noise: TransportState) -> Self {
        Self {
            stream,
            noise,
            buffer: vec![0u8; MAX_BUFFER_SIZE],
        }
    }

    pub async fn from_token(token: &str) -> Result<Self, ConnectionError> {
        let cfg = ConnectionConfig::decode(token).map_err(ConnectionError::Serialization)?;

        if cfg.protocol != ConnectionProtocol::WebSocket || cfg.method != ConnectionMethod::Direct {
            return Err(ConnectionError::InvalidConfig);
        }

        Self::connect(&cfg.addr).await
    }

    pub async fn connect(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        let (mut stream, _response) = connect_async(format!("ws://{}", addr)).await?;
        debug!("Connected to {}", addr);

        let noise = establish_encryption(&mut stream, true).await?;

        debug!("Noise handshake done");
        Ok(WebSocketConnection {
            stream,
            noise,
            buffer: vec![0u8; MAX_BUFFER_SIZE],
        })
    }

    pub async fn send(&mut self, msg: Message) -> Result<(), ConnectionError> {
        let msg = encrypt_message(msg, &mut self.noise, &mut self.buffer)?;
        Ok(self.stream.send(msg).await?)
    }

    pub async fn next(&mut self) -> Result<Message, ConnectionError> {
        let msg = self
            .stream
            .next()
            .await
            .ok_or(ConnectionError::UnexpectedClose)??;
        decrypt_message(msg, &mut self.noise, &mut self.buffer).map_err(ConnectionError::Crypto)
    }

    pub fn split(self) -> (WebSocketSender, WebSocketReceiver) {
        let (sink, stream) = self.stream.split();
        let noise = Arc::new(Mutex::new(self.noise));

        (
            WebSocketSender {
                sink,
                noise: noise.clone(),
                buffer: vec![0u8; MAX_BUFFER_SIZE],
            },
            WebSocketReceiver {
                stream,
                noise,
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
        let noise = Arc::try_unwrap(sender.noise)
            .map_err(|_| ConnectionError::Crypto(CryptoError::InvalidState))?
            .into_inner()
            .map_err(|_| ConnectionError::Crypto(CryptoError::InvalidState))?;
        let stream = SplitSink::reunite(sender.sink, receiver.stream)
            .map_err(|_| ConnectionError::Socket)?;

        Ok(Self {
            stream,
            noise,
            buffer: vec![0u8; MAX_BUFFER_SIZE],
        })
    }

    pub async fn close(&mut self, frame: Option<CloseFrame>) -> Result<(), ConnectionError> {
        Ok(self.stream.close(frame).await?)
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
}

pub struct WebSocketSender {
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    noise: Arc<Mutex<TransportState>>,
    buffer: Vec<u8>,
}

impl WebSocketSender {
    pub async fn send(&mut self, msg: Message) -> Result<(), ConnectionError> {
        let msg = encrypt_message(msg, &mut self.noise.lock().unwrap(), &mut self.buffer)
            .map_err(ConnectionError::Crypto)?;
        Ok(self.sink.send(msg).await?)
    }
}

pub struct WebSocketReceiver {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    noise: Arc<Mutex<TransportState>>,
    buffer: Vec<u8>,
}

impl WebSocketReceiver {
    pub async fn next(&mut self) -> Result<Message, ConnectionError> {
        let msg = self.stream.next().await.unwrap()?;
        decrypt_message(msg, &mut self.noise.lock().unwrap(), &mut self.buffer)
            .map_err(ConnectionError::Crypto)
    }
}
