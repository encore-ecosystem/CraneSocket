use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::socket::{
    ListenerError,
    upnp::{UPnPManager, listeners::upnp::init_upnp},
    utils::ConnectionProtocol,
};

pub mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct UdpListener {
    upnp_manager: UPnPManager,
    socket: UdpSocket,
    external_addr: SocketAddr,
}

impl UdpListener {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = UdpSocket::bind(addr).await?;

        let external_ip = external_ip::get_ipv4().await.unwrap();
        let external_addr = (external_ip.to_string() + ":" + &addr.port().to_string())
            .parse::<SocketAddr>()
            .unwrap();

        let upnp_manager =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Udp).await?;

        Ok(UdpListener {
            upnp_manager,
            socket: listener,
            external_addr,
        })
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), ListenerError> {
        Ok(self.socket.recv_from(buf).await?)
    }

    pub async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> Result<usize, ListenerError> {
        Ok(self.socket.send_to(buf, target).await?)
    }

    pub fn as_raw_listener(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.socket.local_addr()?)
    }

    pub fn get_external_addr(&self) -> SocketAddr {
        self.external_addr
    }
}
