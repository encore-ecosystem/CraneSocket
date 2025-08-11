use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

use crate::socket::ConnectionError;

type WebSocketSplitStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type WebSocketSplitSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

pub struct WebSocketConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WebSocketConnection {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { stream }
    }

    pub async fn connect(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        let (stream, _response) = connect_async(format!("ws://{}", addr)).await?;
        Ok(WebSocketConnection { stream })
    }

    pub async fn send(&mut self, msg: Message) -> Result<usize, ConnectionError> {
        let len = msg.len();
        self.stream.send(msg).await?;
        Ok(len)
    }

    pub async fn next(&mut self) -> Result<Message, ConnectionError> {
        Ok(self.stream.next().await.unwrap()?)
    }

    pub fn split(self) -> (WebSocketSplitSink, WebSocketSplitStream) {
        let (sink, stream) = self.stream.split();
        (sink, stream)
    }

    pub fn reunite(
        sender: WebSocketSplitSink,
        receiver: WebSocketSplitStream,
    ) -> Result<Self, ConnectionError> {
        let stream = SplitSink::reunite(sender, receiver).map_err(|_| ConnectionError::Socket)?;
        Ok(Self { stream })
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
