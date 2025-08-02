use crate::socket::ConnectionError;
use crate::socket::proxy::Message;
use crate::socket::proxy::connections::ws::utils::join_room;
use futures::stream::{SplitSink, SplitStream};
use futures_util::StreamExt;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message as TungsteniteMessage, protocol::CloseFrame},
};

mod logic;
mod utils;

pub struct WebSocketConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WebSocketConnection {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { stream }
    }

    pub async fn connect(addr: &SocketAddr, room_id: String) -> Result<Self, ConnectionError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", addr)).await?;

        join_room(&mut server_conn, room_id).await?;

        let conn = Self::new(server_conn);
        Ok(conn)
    }

    pub async fn send(&mut self, msg: Message) -> Result<(), ConnectionError> {
        logic::send(&mut self.stream, msg).await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Message, ConnectionError> {
        logic::next(&mut self.stream).await
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
}

pub struct WebSocketSender {
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungsteniteMessage>,
}

impl WebSocketSender {
    pub async fn send(&mut self, msg: Message) -> Result<(), ConnectionError> {
        logic::send(&mut self.sink, msg).await?;
        Ok(())
    }
}

pub struct WebSocketReceiver {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl WebSocketReceiver {
    pub async fn next(&mut self) -> Result<Message, ConnectionError> {
        logic::next(&mut self.stream).await
    }
}
