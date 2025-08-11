use std::net::SocketAddr;
use stunclient::StunClient;
use tokio::net::UdpSocket;
use tokio::net::lookup_host;

use crate::socket::StunError;

#[allow(dead_code)]
pub async fn init_with_stun(socket: &UdpSocket) -> Result<SocketAddr, StunError> {
    let external_addr = get_external_addr(socket, None).await?;

    Ok(external_addr)
}

const STUN_HOSTS: [&str; 1] = ["stun.l.google.com:19302"];

pub async fn get_external_addr(
    socket: &UdpSocket,
    stun_servers: Option<Vec<&str>>,
) -> Result<SocketAddr, stunclient::Error> {
    let servers = match stun_servers {
        Some(stun_servers) => {
            stun_servers.clone().extend(STUN_HOSTS);
            stun_servers
        }
        None => Vec::from(STUN_HOSTS),
    };
    for stun_host in servers {
        let stun_addr: SocketAddr = lookup_host(stun_host).await.unwrap().next().unwrap();
        let stun_client = StunClient::new(stun_addr);
        match stun_client.query_external_address_async(socket).await {
            Ok(public_addr) => return Ok(public_addr),
            Err(_) => continue,
        };
    }
    Err(stunclient::Error::Timeout(()))
}
