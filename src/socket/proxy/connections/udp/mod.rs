use log::{debug, error};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::{
    server::constant::MAX_MESSAGE_SIZE,
    socket::{
        ConnectionError,
        config::{ConnectionConfig, ConnectionMethod},
        proxy::common::{is_tag_only_message, validate_tag_with_payload},
        utils::ConnectionProtocol,
    },
};

mod connection_logic;
use connection_logic::*;

pub struct UdpConnection {
    socket: UdpSocket,
    server_addr: Option<SocketAddr>,
    room_id: Option<String>,
}

impl UdpConnection {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ConnectionError> {
        let socket = match UdpSocket::bind(&addr).await {
            Ok(socket) => {
                debug!("Successfully bound socket to {}", socket.local_addr()?);
                socket
            }
            Err(e) => {
                error!("Failed to bind socket to {}: {}", addr, e);
                return Err(ConnectionError::Io(e));
            }
        };

        Ok(Self {
            socket,
            server_addr: None,
            room_id: None,
        })
    }

    pub async fn from_token(&mut self, token: &str) -> Result<(), ConnectionError> {
        let cfg = ConnectionConfig::decode(token).map_err(ConnectionError::Serialization)?;

        if cfg.protocol != ConnectionProtocol::Udp || cfg.method != ConnectionMethod::Proxy {
            return Err(ConnectionError::InvalidConfig);
        }

        let room_id = cfg.room_id.ok_or(ConnectionError::InvalidConfig)?;
        self.join_room(&cfg.addr, room_id.clone()).await
    }

    pub async fn create_room(&mut self, addr: &SocketAddr) -> Result<String, ConnectionError> {
        self.socket.connect(addr).await?;
        debug!("Connected to the proxy server");

        let room_id = register(&mut self.socket).await?;
        debug!("Created a room on the proxy server. room_id={}", room_id);

        self.server_addr = Some(*addr);
        self.room_id = Some(room_id.clone());

        Ok(room_id)
    }

    pub async fn join_room(
        &mut self,
        addr: &SocketAddr,
        room_id: String,
    ) -> Result<(), ConnectionError> {
        self.socket.connect(addr).await?;
        debug!("Connected to the proxy server");

        join_room(&mut self.socket, room_id.clone()).await?;
        debug!("Joined the room on the proxy server");

        self.server_addr = Some(*addr);
        self.room_id = Some(room_id);
        Ok(())
    }

    pub async fn wait_for_client(&mut self) -> Result<(), ConnectionError> {
        wait_for_another_peer(&mut self.socket).await?;
        debug!("Another peer successfully connected to proxy server");
        Ok(())
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
                Err(ConnectionError::Socket)
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
                Err(ConnectionError::Socket)
            }
        }
    }

    pub async fn next(&self) -> Result<Vec<u8>, ConnectionError> {
        let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
        let len = self.socket.recv(&mut buf).await?;

        let data = &buf[..len];

        if data.is_empty() {
            return Err(ConnectionError::UnexpectedMessage(
                "Received empty datagram".into(),
            ));
        }

        let tag = data[0];
        if is_tag_only_message(tag) {
            if len != 1 {
                return Err(ConnectionError::UnexpectedMessage(format!(
                    "Expected exactly 1 byte for tag-only message, got {}",
                    len
                )));
            }
            return Ok(vec![tag]);
        }

        if let Err(msg) = validate_tag_with_payload(tag) {
            return Err(ConnectionError::UnexpectedMessage(msg));
        }

        if len < 9 {
            return Err(ConnectionError::UnexpectedMessage(format!(
                "Message too short, expected at least 9 bytes, got {}",
                len
            )));
        }

        let payload_len = u64::from_be_bytes(data[1..9].try_into().unwrap()) as usize;
        let expected_len = 1 + 8 + payload_len;

        if len != expected_len {
            return Err(ConnectionError::UnexpectedMessage(format!(
                "Expected exactly {} bytes (tag + length + payload), got {}",
                expected_len, len
            )));
        }

        Ok(data.to_vec())
    }

    pub async fn close(&mut self) -> Result<(), ConnectionError> {
        leave_room(&mut self.socket).await
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ConnectionError> {
        Ok(self.socket.local_addr()?)
    }

    pub fn get_token(&self) -> Result<String, ConnectionError> {
        let server_addr = self.server_addr.ok_or(ConnectionError::NotConnected)?;
        let room_id = self.room_id.clone().ok_or(ConnectionError::NotConnected)?;

        let cfg = ConnectionConfig::new(
            ConnectionMethod::Proxy,
            ConnectionProtocol::Udp,
            server_addr,
            Some(room_id),
        );

        cfg.encode().map_err(ConnectionError::Serialization)
    }

    pub fn as_raw_socket(&self) -> &UdpSocket {
        &self.socket
    }
}
