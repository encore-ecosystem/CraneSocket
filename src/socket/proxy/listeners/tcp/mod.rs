use std::net::SocketAddr;
use tokio::net::TcpStream;

use crate::socket::ListenerError;

#[derive(Debug)]
pub struct TcpListener {
    listener: tokio::net::TcpListener,
}

impl TcpListener {
    pub async fn listen(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        Ok(TcpListener { listener })
    }

    pub async fn accept(&self) -> Result<(TcpStream, SocketAddr), ListenerError> {
        Ok(self.listener.accept().await?)
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }
}
