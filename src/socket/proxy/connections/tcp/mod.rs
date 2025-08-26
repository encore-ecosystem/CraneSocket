use log::{debug, error};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    time::timeout,
};

use crate::{
    server::constant::READ_TIMEOUT_MS,
    socket::{
        ConnectionError,
        proxy::common::{is_tag_only_message, validate_tag_with_payload},
    },
};

mod connection_logic;
use connection_logic::*;

pub struct TcpConnection {
    stream: TcpStream,
    room_id: Option<String>,
}

impl TcpConnection {
    pub fn new(stream: TcpStream, room_id: Option<String>) -> Self {
        Self { stream, room_id }
    }

    pub async fn create_room(addr: &SocketAddr) -> Result<(Self, String), ConnectionError> {
        let mut server_conn = TcpStream::connect(addr).await?;
        debug!("Connected to the proxy server");

        let room_id = register(&mut server_conn).await?;
        debug!("Created a room on the proxy server. room_id={}", room_id);

        Ok((Self::new(server_conn, Some(room_id.clone())), room_id))
    }

    pub async fn join_room(addr: &SocketAddr, room_id: String) -> Result<Self, ConnectionError> {
        let mut server_conn = TcpStream::connect(addr).await?;
        debug!("Connected to the proxy server");

        join_room(&mut server_conn, room_id.clone()).await?;
        debug!("Joined the room on the proxy server");

        let conn = Self::new(server_conn, Some(room_id));
        Ok(conn)
    }

    pub async fn wait_for_client(&mut self) -> Result<(), ConnectionError> {
        wait_for_another_client(&mut self.stream).await?;
        debug!("Another peer successfully connected to proxy server");
        Ok(())
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<usize, ConnectionError> {
        Ok(self.stream.write(buf).await?)
    }

    pub async fn send_all(&mut self, buf: &[u8]) -> Result<(), ConnectionError> {
        Ok(self.stream.write_all(buf).await?)
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                format!("Failed to get peer address: {}", e)
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
                Err(ConnectionError::Socket)
            }
        }
    }

    pub async fn recv_exact(&mut self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                format!("Failed to get peer address: {}", e)
            },
            |addr| addr.to_string(),
        );
        match self.stream.read_exact(buf).await {
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

    pub async fn next(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let mut tag = [0u8; 1];
        self.stream.read_exact(&mut tag).await?;

        if is_tag_only_message(tag[0]) {
            return Ok(tag.into());
        }

        if let Err(msg) = validate_tag_with_payload(tag[0]) {
            return Err(ConnectionError::UnexpectedMessage(msg));
        }

        let mut len_buf = [0u8; 8];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u64::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await?;

        let mut data = vec![tag[0]];
        data.extend_from_slice(&len_buf);
        data.extend_from_slice(&payload);

        Ok(data)
    }

    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        let (receiver, sender) = self.stream.into_split();
        (ReadHalf::new(receiver), WriteHalf::new(sender))
    }

    pub async fn flush(&mut self) -> Result<(), ConnectionError> {
        self.stream.flush().await.map_err(ConnectionError::Io)
    }

    pub async fn close(&mut self) -> Result<(), ConnectionError> {
        self.stream.shutdown().await.map_err(ConnectionError::Io)
    }

    pub fn get_token(&self) -> Result<String, ConnectionError> {
        // method(proxy) + server_addr + room_id
        std::unimplemented!()
    }
}

pub struct ReadHalf {
    stream: OwnedReadHalf,
}

impl ReadHalf {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self { stream }
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                format!("Failed to get peer address: {}", e)
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
                Err(ConnectionError::Socket)
            }
        }
    }

    pub async fn recv_exact(&mut self, buf: &mut [u8]) -> Result<usize, ConnectionError> {
        let peer_addr = self.stream.peer_addr().map_or_else(
            |e| {
                log::error!("Failed to get peer address: {}", e);
                format!("Failed to get peer address: {}", e)
            },
            |addr| addr.to_string(),
        );
        match self.stream.read_exact(buf).await {
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

    pub async fn next(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let mut tag = [0u8; 1];
        self.stream.read_exact(&mut tag).await?;

        if is_tag_only_message(tag[0]) {
            return Ok(tag.into());
        }

        if let Err(msg) = validate_tag_with_payload(tag[0]) {
            return Err(ConnectionError::UnexpectedMessage(msg));
        }

        let mut len_buf = [0u8; 8];
        timeout(
            tokio::time::Duration::from_millis(READ_TIMEOUT_MS),
            self.stream.read_exact(&mut len_buf),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::Io)?;
        let len = u64::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        timeout(
            tokio::time::Duration::from_millis(READ_TIMEOUT_MS),
            self.stream.read_exact(&mut payload),
        )
        .await
        .map_err(|_| ConnectionError::Timeout)?
        .map_err(ConnectionError::Io)?;

        let mut data = vec![tag[0]];
        data.extend_from_slice(&len_buf);
        data.extend_from_slice(&payload);

        Ok(data)
    }
}

pub struct WriteHalf {
    stream: OwnedWriteHalf,
}

impl WriteHalf {
    pub fn new(stream: OwnedWriteHalf) -> Self {
        Self { stream }
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<usize, ConnectionError> {
        Ok(self.stream.write(buf).await?)
    }

    pub async fn send_all(&mut self, buf: &[u8]) -> Result<(), ConnectionError> {
        Ok(self.stream.write_all(buf).await?)
    }
}
