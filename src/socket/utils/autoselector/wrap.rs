use std::net::SocketAddr;

use tokio::net::UdpSocket;

use crate::socket::proxy::TcpConnection as ProxyTcpListener;
use crate::socket::proxy::UdpConnection as ProxyUdpConnection;
use crate::socket::proxy::WebSocketConnection as ProxyWebSocketConnection;
use crate::socket::upnp::TcpListener as UPnPTcpListener;
use crate::socket::upnp::UdpListener as UPnPUdpListener;
use crate::socket::upnp::WebSocketListener as UPnPWebSocketListener;
use crate::socket::utils::init_with_stun;
use crate::socket::utils::{
    AutoSelectorConfig, AutoSelectorError, ListeningMethod, auto_select_conn_method,
};

pub enum AutoTcpListener {
    Proxy(ProxyTcpListener),
    UPnP(UPnPTcpListener),
}

impl AutoTcpListener {
    pub async fn listen<'a>(
        addr: &SocketAddr,
        cfg: &AutoSelectorConfig<'a>,
    ) -> Result<Self, AutoSelectorError> {
        match auto_select_conn_method(cfg).await? {
            ListeningMethod::UPnP => {
                let listener = UPnPTcpListener::listen(addr)
                    .await
                    .map_err(|_| AutoSelectorError::Bind)?;
                Ok(Self::UPnP(listener))
            }
            ListeningMethod::Proxy(server_addr) => {
                let (listener, _) = ProxyTcpListener::create_room(&server_addr)
                    .await
                    .map_err(|_| AutoSelectorError::Connect)?;
                Ok(Self::Proxy(listener))
            }
            ListeningMethod::Stun(_) => Err(AutoSelectorError::NoAvailableMethod),
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum AutoWebsocketListener {
    Proxy(ProxyWebSocketConnection),
    UPnP(UPnPWebSocketListener),
}

impl AutoWebsocketListener {
    pub async fn listen<'a>(
        addr: &SocketAddr,
        cfg: &AutoSelectorConfig<'a>,
    ) -> Result<Self, AutoSelectorError> {
        match auto_select_conn_method(cfg).await? {
            ListeningMethod::UPnP => {
                let listener = UPnPWebSocketListener::listen(addr)
                    .await
                    .map_err(|_| AutoSelectorError::Bind)?;
                Ok(Self::UPnP(listener))
            }
            ListeningMethod::Proxy(server_addr) => {
                let (listener, _) = ProxyWebSocketConnection::create_room(&server_addr)
                    .await
                    .map_err(|_| AutoSelectorError::Connect)?;
                Ok(Self::Proxy(listener))
            }
            ListeningMethod::Stun(_) => Err(AutoSelectorError::NoAvailableMethod),
        }
    }
}

pub enum AutoUdpListener {
    Proxy(ProxyUdpConnection),
    UPnP(UPnPUdpListener),
    Stun(UdpSocket),
}

impl AutoUdpListener {
    pub async fn listen<'a>(
        addr: &SocketAddr,
        cfg: &AutoSelectorConfig<'a>,
    ) -> Result<Self, AutoSelectorError> {
        match auto_select_conn_method(cfg).await? {
            ListeningMethod::UPnP => {
                let socket = UPnPUdpListener::bind(addr)
                    .await
                    .map_err(|_| AutoSelectorError::Bind)?;
                Ok(Self::UPnP(socket))
            }
            ListeningMethod::Proxy(server_addr) => {
                let mut socket = ProxyUdpConnection::bind(addr)
                    .await
                    .map_err(|_| AutoSelectorError::Bind)?;
                socket
                    .create_room(&server_addr)
                    .await
                    .map_err(|_| AutoSelectorError::Connect)?;
                Ok(Self::Proxy(socket))
            }
            ListeningMethod::Stun(host) => {
                let socket = UdpSocket::bind(&addr)
                    .await
                    .map_err(|_| AutoSelectorError::Bind)?;
                init_with_stun(&socket, Some(vec![&host]))
                    .await
                    .map_err(|_| AutoSelectorError::Connect)?;

                Ok(Self::Stun(socket))
            }
        }
    }
}
