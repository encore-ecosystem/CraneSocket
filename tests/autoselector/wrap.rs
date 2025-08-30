use std::net::SocketAddr;

use crane_socket::{
    server::{
        TcpProxyServer, UdpProxyServer, WebSocketProxyServer,
        message::{ClientMessage, ServerMessage},
    },
    socket::utils::{
        AutoSelectorConfig, AutoTcpListener, AutoUdpListener, AutoWebsocketListener,
        ConnectionProtocol, ListeningMethod, auto_select_conn_method,
    },
};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Bytes;

use crate::{TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_auto_tcp_proxy() {
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

    assert_eq!(method, ListeningMethod::Proxy(server_addr));

    let stream = AutoTcpListener::listen(&bind_addr, &cfg).await.unwrap();

    let mut stream = match stream {
        AutoTcpListener::Proxy(stream) => stream,
        AutoTcpListener::UPnP(_) => {
            panic!("Expected ProxyTcpListener, got UPnPTcpListener")
        }
    };

    let frame = Bytes::from("123");

    stream
        .send(&ClientMessage::Ping(frame.clone()).as_bytes())
        .await
        .unwrap();

    let data = timed(stream.next()).await.unwrap();
    let received_msg = ServerMessage::try_from(&data[..]).unwrap();

    assert_eq!(received_msg, ServerMessage::Pong(frame));

    shutdown_tx.send(()).unwrap();
    stream.close().await.unwrap();
}

#[tokio::test]
async fn test_auto_websocket_proxy() {
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

    assert_eq!(method, ListeningMethod::Proxy(server_addr));

    let stream = AutoWebsocketListener::listen(&bind_addr, &cfg)
        .await
        .unwrap();

    let mut stream = match stream {
        AutoWebsocketListener::Proxy(stream) => stream,
        AutoWebsocketListener::UPnP(_) => {
            panic!("Expected ProxyWebSocketListener, got UPnPWebSocketListener")
        }
    };

    let frame = Bytes::from("123");

    stream
        .send(ClientMessage::Ping(frame.clone()))
        .await
        .unwrap();

    let received_msg = timed(stream.next()).await.unwrap();

    assert_eq!(received_msg, ServerMessage::Pong(frame));

    shutdown_tx.send(()).unwrap();
    stream.close().await.unwrap();
}

#[tokio::test]
async fn test_auto_udp_proxy() {
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

    assert_eq!(method, ListeningMethod::Proxy(server_addr));

    let socket = AutoUdpListener::listen(&bind_addr, &cfg).await.unwrap();

    let mut stream = match socket {
        AutoUdpListener::Proxy(stream) => stream,
        _ => {
            panic!("Expected ProxyUdpListener")
        }
    };

    let frame = Bytes::from("123");

    stream
        .send(&ClientMessage::Ping(frame.clone()).as_bytes())
        .await
        .unwrap();

    let data = timed(stream.next()).await.unwrap();
    let received_msg = ServerMessage::try_from(&data[..]).unwrap();

    assert_eq!(received_msg, ServerMessage::Pong(frame));

    shutdown_tx.send(()).unwrap();
    stream.close().await.unwrap();
}
