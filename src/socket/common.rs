use std::net::SocketAddrV4;

use igd::{SearchError, SearchOptions, search_gateway};

use tokio::net::UdpSocket;

pub const STUN_HOSTS: [&str; 1] = ["stun.l.google.com:19302"];

pub async fn get_default_gateway() -> std::io::Result<std::net::IpAddr> {
    let remote = "8.8.8.8:80";
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let _ = socket.connect(remote).await;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip())
}

pub async fn get_default_gateway_with_upnp() -> Result<SocketAddrV4, SearchError> {
    match search_gateway(SearchOptions::default()) {
        Ok(gateway) => Ok(gateway.addr),
        Err(e) => Err(e),
    }
}
