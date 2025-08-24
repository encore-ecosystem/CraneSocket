use std::net::SocketAddr;

use crate::{
    server::message::{ClientMessage, ServerMessage},
    socket::{
        common::STUN_HOSTS,
        proxy::{TcpConnection, UdpConnection, WebSocketConnection},
        utils::ConnectionProtocol,
    },
};

use futures::future::join_all;
use igd::{SearchOptions, search_gateway};
use log::info;
use stunclient::StunClient;
use thiserror::Error;
use tokio::{
    net::{TcpStream, UdpSocket, lookup_host},
    time::{Duration, Instant, timeout},
};
use tokio_tungstenite::tungstenite::Bytes;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AutoSelectorConfig<'a> {
    protocol: ConnectionProtocol,
    upnp: bool,
    stun: Vec<&'a str>,
    proxy: Vec<SocketAddr>,
}

impl<'a> AutoSelectorConfig<'a> {
    pub fn new(protocol: ConnectionProtocol) -> Self {
        Self {
            protocol,
            upnp: true,
            stun: Vec::new(),
            proxy: Vec::new(),
        }
    }

    pub fn upnp(mut self, enabled: bool) -> Self {
        self.upnp = enabled;
        self
    }

    pub fn stun_servers(mut self, servers: Vec<&'a str>) -> Self {
        self.stun = servers;
        self
    }

    pub fn add_stun_server(mut self, server: &'a str) -> Self {
        self.stun.push(server);
        self
    }

    pub fn proxy_servers(mut self, servers: Vec<SocketAddr>) -> Self {
        self.proxy = servers;
        self
    }

    pub fn add_proxy_server(mut self, server: SocketAddr) -> Self {
        self.proxy.push(server);
        self
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionMethod {
    UPnP,
    Stun(String),
    Proxy(SocketAddr),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum AutoSelectorError {
    #[error("UPnPNotAvailable error")]
    UPnPNotAvailable,
    #[error("UPnPPortMapFailed error")]
    UPnPPortMapFailed,
    #[error("StunConnectionFailed error")]
    StunConnectionFailed,
    #[error("NoAvailableProxy error")]
    NoAvailableProxy,
    #[error("ServerUnreachable error")]
    ServerUnreachable,
    #[error("NoAvailableMethod error")]
    NoAvailableMethod,
    #[error("Bind error")]
    Bind,
    #[error("Connect error")]
    Connect,
    #[error("Send error")]
    Send,
    #[error("Receive error")]
    Receive,
    #[error("Timeout")]
    Timeout,
    #[error("InvalidResponse {0}")]
    InvalidResponse(String),
    #[error("InvalidHost {0}")]
    InvalidHost(String),
}

pub fn is_upnp_available() -> bool {
    match search_gateway(SearchOptions::default()) {
        Ok(gateway) => gateway.get_external_ip().is_ok(),
        Err(_) => false,
    }
}

pub async fn is_proxy_available(
    protocol: &ConnectionProtocol,
    servers: &[SocketAddr],
) -> Result<SocketAddr, AutoSelectorError> {
    let checks = servers.iter().map(|&addr| ping_server(protocol, addr));
    let results = join_all(checks).await;

    let best = results
        .into_iter()
        .filter_map(|res| res.ok())
        .min_by_key(|(_, dur)| *dur);

    best.map(|(addr, _)| addr)
        .ok_or(AutoSelectorError::NoAvailableProxy)
}

async fn ping_server(
    protocol: &ConnectionProtocol,
    server_addr: SocketAddr,
) -> Result<(SocketAddr, Duration), AutoSelectorError> {
    let start = Instant::now();
    let result = match protocol {
        ConnectionProtocol::Tcp => check_tcp_proxy(server_addr).await,
        ConnectionProtocol::Udp => check_udp_proxy(server_addr).await,
        ConnectionProtocol::WebSocket => check_ws_proxy(server_addr).await,
    };
    result.map(|_| (server_addr, start.elapsed()))
}

async fn check_tcp_proxy(server_addr: SocketAddr) -> Result<(), AutoSelectorError> {
    let stream = TcpStream::connect(server_addr)
        .await
        .map_err(|_| AutoSelectorError::ServerUnreachable)?;
    let mut conn = TcpConnection::new(stream);

    conn.send(&ClientMessage::Ping(Bytes::new()).as_bytes())
        .await
        .map_err(|_| AutoSelectorError::Send)?;

    let data = timeout(Duration::from_secs(1), conn.next())
        .await
        .map_err(|_| AutoSelectorError::Timeout)
        .map_err(|_| AutoSelectorError::Receive)?
        .map_err(|e| AutoSelectorError::InvalidResponse(format!("{:?}", e)))?;

    match ServerMessage::try_from(&data[..]) {
        Ok(ServerMessage::Pong(_)) => Ok(()),
        msg => Err(AutoSelectorError::InvalidResponse(format!("{:?}", msg))),
    }
}

async fn check_udp_proxy(server_addr: SocketAddr) -> Result<(), AutoSelectorError> {
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let socket = UdpConnection::bind(&bind_addr)
        .await
        .map_err(|_| AutoSelectorError::Bind)?;
    socket
        .as_raw_socket()
        .connect(server_addr)
        .await
        .map_err(|_| AutoSelectorError::ServerUnreachable)?;
    socket
        .send(&ClientMessage::Ping(Bytes::new()).as_bytes())
        .await
        .map_err(|_| AutoSelectorError::Send)?;

    let data = timeout(Duration::from_secs(1), socket.next())
        .await
        .map_err(|_| AutoSelectorError::Timeout)
        .map_err(|_| AutoSelectorError::Receive)?
        .map_err(|e| AutoSelectorError::InvalidResponse(format!("{:?}", e)))?;

    match ServerMessage::try_from(&data[..]) {
        Ok(ServerMessage::Pong(_)) => Ok(()),
        msg => Err(AutoSelectorError::InvalidResponse(format!("{:?}", msg))),
    }
}

async fn check_ws_proxy(server_addr: SocketAddr) -> Result<(), AutoSelectorError> {
    let url = format!("ws://{}", server_addr);
    let (stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|_| AutoSelectorError::Connect)?;
    let mut conn = WebSocketConnection::new(stream);

    conn.send(ClientMessage::Ping(Bytes::new()))
        .await
        .map_err(|_| AutoSelectorError::Send)?;

    let msg = timeout(Duration::from_secs(1), conn.next())
        .await
        .map_err(|_| AutoSelectorError::Timeout)?
        .map_err(|_| AutoSelectorError::Receive)?;

    match msg {
        ServerMessage::Pong(_) => Ok(()),
        msg => Err(AutoSelectorError::InvalidResponse(format!("{:?}", msg))),
    }
}

pub async fn is_stun_available(stun_servers: &[&str]) -> Result<String, AutoSelectorError> {
    let servers: Vec<&str> = if stun_servers.is_empty() {
        STUN_HOSTS.to_vec()
    } else {
        let mut list = stun_servers.to_vec();
        list.extend(STUN_HOSTS.iter());
        list
    };

    let local_socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|_| AutoSelectorError::Bind)?;

    for host in servers {
        let mut addrs_iter = lookup_host(&host)
            .await
            .map_err(|_| AutoSelectorError::InvalidHost(host.to_string()))?;
        if let Some(stun_addr) = addrs_iter.next() {
            let stun_client = StunClient::new(stun_addr);
            let result = timeout(
                Duration::from_secs(2),
                stun_client.query_external_address_async(&local_socket),
            )
            .await;
            match result {
                Ok(Ok(_)) => return Ok(host.to_string()),
                _ => continue,
            }
        }
    }
    Err(AutoSelectorError::Timeout)
}

pub async fn auto_select_conn_method<'a>(
    cfg: &AutoSelectorConfig<'a>,
) -> Result<ConnectionMethod, AutoSelectorError> {
    match cfg.protocol {
        ConnectionProtocol::Tcp => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ConnectionMethod::UPnP);
            }

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ConnectionMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }

            Err(AutoSelectorError::NoAvailableMethod)
        }
        ConnectionProtocol::Udp => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ConnectionMethod::UPnP);
            }

            if !cfg.stun.is_empty() {
                match is_stun_available(&cfg.stun).await {
                    Ok(addr) => return Ok(ConnectionMethod::Stun(addr)),
                    Err(e) => {
                        info!("Could not connect to stun: {}", e);
                    }
                }
            }

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ConnectionMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }

            Err(AutoSelectorError::NoAvailableMethod)
        }
        ConnectionProtocol::WebSocket => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ConnectionMethod::UPnP);
            }

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ConnectionMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }

            Err(AutoSelectorError::NoAvailableMethod)
        }
    }
}
