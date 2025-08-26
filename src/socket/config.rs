use std::net::SocketAddr;

use base64::{Engine as _, engine::general_purpose};
use postcard;
use serde::{Deserialize, Serialize};

use crate::socket::utils::ConnectionProtocol;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ConnectionMethod {
    Direct,
    Proxy,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub method: ConnectionMethod,
    pub protocol: ConnectionProtocol,
    pub addr: SocketAddr,
    pub room_id: Option<String>,
}

impl ConnectionConfig {
    pub fn new(
        method: ConnectionMethod,
        protocol: ConnectionProtocol,
        addr: SocketAddr,
        room_id: Option<String>,
    ) -> Self {
        ConnectionConfig {
            method,
            protocol,
            addr,
            room_id,
        }
    }

    pub fn encode(&self) -> Result<String, postcard::Error> {
        let bin = postcard::to_allocvec(self)?;
        Ok(general_purpose::STANDARD.encode(bin))
    }

    pub fn decode(s: &str) -> Result<Self, postcard::Error> {
        let bin = general_purpose::STANDARD
            .decode(s)
            .map_err(|_| postcard::Error::DeserializeBadEncoding)?;
        postcard::from_bytes(&bin)
    }
}
