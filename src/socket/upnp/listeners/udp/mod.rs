use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::socket::{
    ListenerError,
    upnp::{
        UPnPManager,
        error::UPnPManagerError,
        listeners::{
            ConnectionMethod, ConnectionProtocol, udp::utils::init_with_stun, upnp::init_upnp,
        },
    },
};

mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct UdpListener {
    upnp_manager: Option<UPnPManager>,
    listener: UdpSocket,
    external_addr: SocketAddr,
    conn_type: ConnectionMethod,
}

impl UdpListener {
    pub async fn bind(addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = UdpSocket::bind(addr).await?;

        let external_ip = external_ip::get_ipv4().await.unwrap();
        let external_addr = (external_ip.to_string() + ":" + &addr.port().to_string())
            .parse::<SocketAddr>()
            .unwrap();

        if let Ok(upnp_manager) =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Udp).await
        {
            return Ok(UdpListener {
                upnp_manager: Some(upnp_manager),
                listener,
                external_addr,
                conn_type: ConnectionMethod::UPnP,
            });
        }

        if let Ok(external_addr) = init_with_stun(&listener).await {
            return Ok(UdpListener {
                upnp_manager: None,
                listener,
                external_addr,
                conn_type: ConnectionMethod::Stun,
            });
        }

        Err(ListenerError::Upnp(UPnPManagerError::Upnp(
            easy_upnp::Error::CannotGetInterfaceAddress(std::io::Error::other(
                "Could not bind socket",
            )),
        )))
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), ListenerError> {
        Ok(self.listener.recv_from(buf).await?)
    }

    pub async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> Result<usize, ListenerError> {
        Ok(self.listener.send_to(buf, target).await?)
    }

    pub fn as_raw_listener(&self) -> &UdpSocket {
        &self.listener
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn get_external_addr(&self) -> SocketAddr {
        self.external_addr
    }
}
