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
    // pub public_key: 
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

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose};
    use postcard;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn default_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
    }

    #[test]
    fn test_new_direct_without_room_id() {
        let addr = default_addr();
        let config = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            addr,
            None,
        );

        assert_eq!(config.method, ConnectionMethod::Direct);
        assert_eq!(config.protocol, ConnectionProtocol::Tcp);
        assert_eq!(config.addr, addr);
        assert_eq!(config.room_id, None);
    }

    #[test]
    fn test_new_direct_with_room_id() {
        let addr = default_addr();
        let config = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            addr,
            Some("test_room".to_string()),
        );

        assert_eq!(config.method, ConnectionMethod::Direct);
        assert_eq!(config.protocol, ConnectionProtocol::Tcp);
        assert_eq!(config.addr, addr);
        assert_eq!(config.room_id, Some("test_room".to_string()));
    }

    #[test]
    fn test_new_proxy_with_room_id() {
        let addr = default_addr();
        let config = ConnectionConfig::new(
            ConnectionMethod::Proxy,
            ConnectionProtocol::Tcp,
            addr,
            Some("test_room".to_string()),
        );

        assert_eq!(config.method, ConnectionMethod::Proxy);
        assert_eq!(config.protocol, ConnectionProtocol::Tcp);
        assert_eq!(config.addr, addr);
        assert_eq!(config.room_id, Some("test_room".to_string()));
    }

    #[test]
    fn test_new_proxy_without_room_id() {
        let addr = default_addr();
        let config =
            ConnectionConfig::new(ConnectionMethod::Proxy, ConnectionProtocol::Udp, addr, None);

        assert_eq!(config.method, ConnectionMethod::Proxy);
        assert_eq!(config.protocol, ConnectionProtocol::Udp);
        assert_eq!(config.addr, addr);
        assert_eq!(config.room_id, None);
    }

    #[test]
    fn test_encode_decode_valid_config() {
        let config = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            default_addr(),
            Some("room123".to_string()),
        );
        let encoded = config.encode().expect("Encoding should succeed");
        let decoded = ConnectionConfig::decode(&encoded).expect("Decoding should succeed");

        assert_eq!(config, decoded);
    }

    #[test]
    fn test_encode_decode_no_room_id() {
        let config = ConnectionConfig::new(
            ConnectionMethod::Proxy,
            ConnectionProtocol::Tcp,
            default_addr(),
            None,
        );
        let encoded = config.encode().expect("Encoding should succeed");
        let decoded = ConnectionConfig::decode(&encoded).expect("Decoding should succeed");

        assert_eq!(config, decoded);
        assert_eq!(decoded.room_id, None);
    }

    #[test]
    fn test_decode_invalid_base64() {
        let invalid_base64 = "invalid_base64_string!";
        let result = ConnectionConfig::decode(invalid_base64);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), postcard::Error::DeserializeBadEncoding);
    }

    #[test]
    fn test_decode_invalid_postcard_data() {
        let invalid_data = vec![0u8, 1u8, 2u8, 3u8];
        let encoded = general_purpose::STANDARD.encode(&invalid_data);
        let result = ConnectionConfig::decode(&encoded);

        assert!(result.is_err());
    }

    #[test]
    fn test_encode_large_room_id() {
        let config = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            default_addr(),
            Some("very_long_room_id_".repeat(100)),
        );
        let result = config.encode();

        assert!(result.is_ok(), "Encoding should handle large room IDs");
    }

    #[test]
    fn test_equality_different_configs() {
        let config1 = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            default_addr(),
            Some("room1".to_string()),
        );
        let config2 = ConnectionConfig::new(
            ConnectionMethod::Proxy,
            ConnectionProtocol::Udp,
            default_addr(),
            Some("room1".to_string()),
        );

        assert_ne!(
            config1, config2,
            "Configs with different methods/protocols should not be equal"
        );
    }

    #[test]
    fn test_encode_decode_empty_room_id() {
        let config = ConnectionConfig::new(
            ConnectionMethod::Direct,
            ConnectionProtocol::Tcp,
            default_addr(),
            Some("".to_string()),
        );
        let encoded = config.encode().expect("Encoding should succeed");
        let decoded = ConnectionConfig::decode(&encoded).expect("Decoding should succeed");

        assert_eq!(config, decoded);
        assert_eq!(decoded.room_id, Some("".to_string()));
    }
}
