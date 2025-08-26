use std::net::SocketAddr;
use std::str::FromStr;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, accept_async};

use crate::socket::ListenerError;
use crate::socket::config::{ConnectionConfig, ConnectionMethod};
use crate::socket::upnp::UPnPManager;
use crate::socket::upnp::listeners::upnp::init_upnp;
use crate::socket::utils::ConnectionProtocol;

#[allow(dead_code)]
#[derive(Debug)]
pub struct WebSocketListener {
    upnp_manager: Option<UPnPManager>,
    listener: tokio::net::TcpListener,
}

impl WebSocketListener {
    pub async fn listen(local_addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&local_addr).await?;
        let upnp_manager =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Tcp).await?;

        Ok(Self {
            upnp_manager: Some(upnp_manager),
            listener,
        })
    }

    pub async fn accept(&self) -> Result<(WebSocketStream<TcpStream>, SocketAddr), ListenerError> {
        let (stream, addr) = self.listener.accept().await?;
        let stream = accept_async(stream).await?;
        Ok((stream, addr))
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn get_external_addr(&self) -> Result<SocketAddr, ListenerError> {
        let ip = external_ip::get_ipv4().await.ok_or(ListenerError::Socket)?;
        let port = self.listener.local_addr()?.port();
        let addr = SocketAddr::from_str(&(ip.to_string() + ":" + &port.to_string()))
            .map_err(|_| ListenerError::Socket)?;
        Ok(addr)
    }

    pub async fn get_token(&self) -> Result<String, ListenerError> {
        let server_addr = self.get_external_addr().await?;
        let cfg = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::WebSocket,
            server_addr,
            None,
        );
        cfg.encode().map_err(ListenerError::Serialization)
    }
}
