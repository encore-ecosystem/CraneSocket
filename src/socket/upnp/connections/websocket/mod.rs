use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::socket::ConnectionError;

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
}
