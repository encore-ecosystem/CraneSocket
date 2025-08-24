use std::net::SocketAddr;

use tokio::sync::oneshot;
use upnpsocket::{
    server::{TcpProxyServer, UdpProxyServer, WebSocketProxyServer},
    socket::{
        common::STUN_HOSTS,
        utils::{
            AutoSelectorConfig, AutoSelectorError, ConnectionMethod, ConnectionProtocol,
            auto_select_conn_method,
        },
    },
};

use crate::{TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_autoselector_tcp_proxy_success() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server = timed(TcpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Tcp)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ConnectionMethod::Proxy(server_addr));

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_autoselector_no_available_tcp_proxy() {
    let server_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Tcp)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Tcp).upnp(false);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }
}

#[tokio::test]
async fn test_autoselector_websocket_proxy_success() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::WebSocket)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ConnectionMethod::Proxy(server_addr));

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_autoselector_no_available_websocket_proxy() {
    let server_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::WebSocket)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::WebSocket).upnp(false);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }
}

#[tokio::test]
async fn test_autoselector_udp_proxy_success() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server = timed(UdpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ConnectionMethod::Proxy(server_addr));

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_autoselector_no_available_udp_proxy() {
    let server_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp).upnp(false);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }
}

#[tokio::test]
async fn test_autoselector_wrong_protocol() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let websocket_server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = websocket_server.get_local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        websocket_server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .add_proxy_server(server_addr);
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_autoselector_mixed_proxy_protocols() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let websocket_server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let websocket_server_addr = websocket_server.get_local_addr().unwrap();
    let (websocket_shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        websocket_server.serve(shutdown_rx).await.unwrap();
    });
    let udp_server = timed(UdpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let udp_server_addr = udp_server.get_local_addr().unwrap();
    let (udp_shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        udp_server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .add_proxy_server(websocket_server_addr)
        .add_proxy_server(udp_server_addr);
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ConnectionMethod::Proxy(udp_server_addr));

    websocket_shutdown_tx.send(()).unwrap();
    udp_shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_autoselector_stun_success() {
    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .stun_servers(STUN_HOSTS.to_vec());
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ConnectionMethod::Stun(STUN_HOSTS[0].to_owned()));
}

#[tokio::test]
async fn test_autoselector_stun_invalid_host() {
    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .stun_servers(Vec::from(["123"]));
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }
}
