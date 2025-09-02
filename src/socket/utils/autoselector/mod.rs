use std::net::SocketAddr;

use crate::socket::utils::ConnectionProtocol;

use log::{debug, info};

mod error;
mod logic;
mod stun;
mod wrap;

pub use error::*;
use logic::*;
pub use stun::*;
pub use wrap::*;

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
pub enum ListeningMethod {
    UPnP,
    Stun(String),
    Proxy(SocketAddr),
}

pub async fn auto_select_conn_method<'a>(
    cfg: &AutoSelectorConfig<'a>,
) -> Result<ListeningMethod, AutoSelectorError> {
    match cfg.protocol {
        ConnectionProtocol::Tcp => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ListeningMethod::UPnP);
            }
            debug!("Could not use UPnP");

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ListeningMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }
            debug!("Could not use proxy");

            Err(AutoSelectorError::NoAvailableMethod)
        }
        ConnectionProtocol::Udp => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ListeningMethod::UPnP);
            }
            debug!("Could not use UPnP");

            if !cfg.stun.is_empty() {
                match is_stun_available(&cfg.stun).await {
                    Ok(addr) => return Ok(ListeningMethod::Stun(addr)),
                    Err(e) => {
                        info!("Could not connect to stun: {}", e);
                    }
                }
            }
            debug!("Could not use STUN");

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ListeningMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }
            debug!("Could not use proxy");

            Err(AutoSelectorError::NoAvailableMethod)
        }
        ConnectionProtocol::WebSocket => {
            if cfg.upnp && is_upnp_available() {
                return Ok(ListeningMethod::UPnP);
            }
            debug!("Could not use UPnP");

            if !cfg.proxy.is_empty() {
                match is_proxy_available(&cfg.protocol, &cfg.proxy).await {
                    Ok(addr) => return Ok(ListeningMethod::Proxy(addr)),
                    Err(e) => {
                        info!("Could not connect to proxy: {}", e);
                    }
                }
            }
            debug!("Could not use proxy");

            Err(AutoSelectorError::NoAvailableMethod)
        }
    }
}
