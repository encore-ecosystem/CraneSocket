use std::{net::SocketAddr, str::FromStr};
use tokio::net::UdpSocket;

use crate::socket::{
    ListenerError,
    config::{ConnectionConfig, ConnectionMethod},
    upnp::{UPnPManager, listeners::upnp::init_upnp},
    utils::ConnectionProtocol,
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct UdpListener {
    upnp_manager: UPnPManager,
    socket: UdpSocket,
}

impl UdpListener {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = UdpSocket::bind(addr).await?;
        let upnp_manager =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Udp).await?;
        Ok(UdpListener {
            upnp_manager,
            socket: listener,
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

    pub async fn get_external_addr(&self) -> Result<SocketAddr, ListenerError> {
        let ip = external_ip::get_ipv4().await.ok_or(ListenerError::Socket)?;
        let port = self.socket.local_addr()?.port();
        let addr = SocketAddr::from_str(&(ip.to_string() + ":" + &port.to_string()))
            .map_err(|_| ListenerError::Socket)?;
        Ok(addr)
    }

    pub async fn get_token(&self) -> Result<String, ListenerError> {
        let server_addr = self.get_external_addr().await?;
        let cfg = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Udp,
            server_addr,
            None,
        );
        cfg.encode().map_err(ListenerError::Serialization)
    }
}
