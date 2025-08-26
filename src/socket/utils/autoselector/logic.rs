use std::net::SocketAddr;

use crate::{
    server::message::{ClientMessage, ServerMessage},
    socket::{
        common::STUN_HOSTS,
        proxy::{TcpConnection, UdpConnection, WebSocketConnection},
        utils::{ConnectionProtocol, autoselector::error::AutoSelectorError},
    },
};

use futures::future::join_all;
use igd::{SearchOptions, search_gateway};
use stunclient::StunClient;
use tokio::{
    net::{TcpStream, UdpSocket, lookup_host},
    time::{Duration, Instant, timeout},
};
use tokio_tungstenite::tungstenite::Bytes;

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
    let mut conn = TcpConnection::new(stream, None);

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
