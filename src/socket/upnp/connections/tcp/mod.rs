use log::{debug, error};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::socket::ConnectionError;

pub mod utils;

pub struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn connect(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                debug!("Successfully connected to {}", addr);
                Ok(TcpConnection { stream })
            }
            Err(e) => {
                error!("Failed to connected to {}: {}", addr, e);
                Err(ConnectionError::Socket(e))
            }
        }
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                "unknown".to_string()
            },
            |addr| addr.to_string(),
        );

        match self.stream.write_all(buf).await {
            Ok(()) => {
                debug!("Sent {} bytes to peer {}", buf.len(), peer_addr);
                Ok(buf.len())
            }
            Err(e) => {
                error!("Failed to send to peer {}: {}", peer_addr, e);
                Err(ConnectionError::Io)
            }
        }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                "unknown".to_string()
            },
            |addr| addr.to_string(),
        );
        match self.stream.read(buf).await {
            Ok(bytes_read) => {
                debug!("Read {} bytes from peer {}", bytes_read, peer_addr);
                Ok(bytes_read)
            }
            Err(e) => {
                error!("Failed to read from peer {}: {}", peer_addr, e);
                Err(ConnectionError::Io)
            }
        }
    }
}
