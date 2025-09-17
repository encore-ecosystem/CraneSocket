use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, accept_async};

use crate::socket::ListenerError;

#[derive(Debug)]
pub struct WebSocketListener {
    listener: tokio::net::TcpListener,
}

impl WebSocketListener {
    pub async fn listen(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        Ok(Self { listener })
    }

    pub async fn accept(&self) -> Result<(WebSocketStream<TcpStream>, SocketAddr), ListenerError> {
        let (stream, addr) = self.listener.accept().await?;
        let stream = accept_async(stream).await?;
        Ok((stream, addr))
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }
}
