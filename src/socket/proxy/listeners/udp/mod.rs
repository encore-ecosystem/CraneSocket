use log::{debug, error};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::{
    server::constant::MAX_MESSAGE_SIZE,
    socket::{
        ListenerError,
        proxy::common::{is_tag_only_message, validate_tag_with_payload},
    },
};

#[derive(Debug)]
pub struct UdpListener {
    socket: UdpSocket,
}

impl UdpListener {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = UdpSocket::bind(addr).await?;

        Ok(UdpListener { socket: listener })
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), ListenerError> {
        match self.socket.recv_from(buf).await {
            Ok((bytes_sent, addr)) => {
                debug!("Received {} bytes from {}", bytes_sent, addr);
                Ok((bytes_sent, addr))
            }
            Err(e) => {
                error!("Failed to receive: {}", e);
                Err(ListenerError::Socket)
            }
        }
    }

    pub async fn next_from(&self) -> Result<(Vec<u8>, SocketAddr), ListenerError> {
        let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
        let (len, addr) = self.socket.recv_from(&mut buf).await?;

        let data = &buf[..len];

        if data.is_empty() {
            error!("Received empty datagram");
            return Err(ListenerError::InvalidDatagramFrom(addr));
        }

        let tag = data[0];
        if is_tag_only_message(tag) {
            if len != 1 {
                error!("Expected exactly 1 byte for tag-only message, got {}", len);
                return Err(ListenerError::InvalidDatagramFrom(addr));
            }
            return Ok((vec![tag], addr));
        }

        if let Err(msg) = validate_tag_with_payload(tag) {
            return Err(ListenerError::UnexpectedMessage(msg));
        }

        if len < 9 {
            error!("Message too short, expected at least 9 bytes, got {}", len);
            return Err(ListenerError::InvalidDatagramFrom(addr));
        }

        let payload_len = u64::from_be_bytes(data[1..9].try_into().unwrap()) as usize;
        let expected_len = 1 + 8 + payload_len;

        if len != expected_len {
            error!(
                "Expected exactly {} bytes (tag + length + payload), got {}",
                expected_len, len
            );
            return Err(ListenerError::InvalidDatagramFrom(addr));
        }

        Ok((data.to_vec(), addr))
    }

    pub async fn send_to(&self, buf: &[u8], addr: &SocketAddr) -> Result<usize, ListenerError> {
        match self.socket.send_to(buf, addr).await {
            Ok(bytes_sent) => {
                debug!("Sent {} bytes to {}", bytes_sent, addr);
                Ok(bytes_sent)
            }
            Err(e) => {
                error!("Failed to send to {}: {}", addr, e);
                Err(ListenerError::Socket)
            }
        }
    }

    pub fn as_raw_listener(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.socket.local_addr()?)
    }
}
