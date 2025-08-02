use log::{debug, error};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::socket::ConnectionError;

#[allow(dead_code)]
pub struct UdpConnection {
    socket: UdpSocket,
}

impl UdpConnection {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        let socket = match UdpSocket::bind(&addr).await {
            Ok(socket) => {
                debug!("Successfully bound socket to {}", addr);
                socket
            }
            Err(e) => {
                error!("Failed to connected to {}: {}", addr, e);
                return Err(ConnectionError::Socket(e));
            }
        };

        Ok(UdpConnection { socket })
    }

    pub async fn connect(&self, addr: &SocketAddr) -> Result<(), ConnectionError> {
        // match self.socket.connect(addr).await {
        //     Ok(()) => {
        //         debug!("Successfully connected to {}", addr);
        //         Ok(())
        //     }
        //     Err(e) => {
        //         error!("Failed to connected to {}", addr);
        //         Err(ConnectionError::Socket(e))
        //     }
        // }

        unimplemented!()
    }

    pub async fn send(&self, buf: &[u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.socket.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                "unknown".to_string()
            },
            |addr| addr.to_string(),
        );

        match self.socket.send(buf).await {
            Ok(bytes_sent) => {
                debug!("Sent {} bytes to peer {}", bytes_sent, peer_addr);
                Ok(bytes_sent)
            }
            Err(e) => {
                error!("Failed to send to peer {}: {}", peer_addr, e);
                Err(ConnectionError::Io)
            }
        }
    }

    pub async fn recv(&self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.socket.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                "unknown".to_string()
            },
            |addr| addr.to_string(),
        );
        match self.socket.recv(buf).await {
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
