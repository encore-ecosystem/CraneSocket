use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::socket::ListenerError;

mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct UdpListener {
    listener: UdpSocket,
}

impl UdpListener {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ListenerError> {
        // let listener = UdpSocket::bind(addr).await?;

        // Ok(Self { listener })

        unimplemented!()
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), ListenerError> {
        Ok(self.listener.recv_from(buf).await?)
    }

    pub async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> Result<usize, ListenerError> {
        Ok(self.listener.send_to(buf, target).await?)
    }

    pub fn raw_listener(&self) -> &UdpSocket {
        &self.listener
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }
}
