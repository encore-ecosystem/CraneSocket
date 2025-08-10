use std::net::SocketAddr;

use tokio::sync::oneshot;
use upnpsocket::{
    server::{TcpProxyServer, message::ServerMessage},
    socket::proxy::TcpConnection,
};

use crate::{TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_create_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(TcpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let peer2room = server.get_peer2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (mut first_conn, _) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(peer2room.read().await.len(), 1);

    let (mut second_conn, _) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 2);
    assert_eq!(peer2room.read().await.len(), 2);

    timed(first_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(peer2room.read().await.len(), 1);

    timed(second_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(peer2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_create_and_join_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(TcpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let peer2room = server.get_peer2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (mut create_room_conn, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(peer2room.read().await.len(), 1);

    let mut join_room_conn = timed(TcpConnection::join_room(&server_addr, room_id))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(peer2room.read().await.len(), 2);

    let msg = timed(create_room_conn.next()).await.unwrap();

    assert_eq!(msg, ServerMessage::ClientJoined.as_bytes());

    timed(join_room_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(peer2room.read().await.len(), 1);

    timed(create_room_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(peer2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_join_nonexisting_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(TcpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let peer2room = server.get_peer2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(peer2room.read().await.len(), 0);

    let room_id = "123".to_string();
    let res = timed(TcpConnection::join_room(&server_addr, room_id))
        .await
        .is_err();

    assert!(res);
    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(peer2room.read().await.len(), 0);

    let room_id = "".to_string();
    let res = timed(TcpConnection::join_room(&server_addr, room_id))
        .await
        .is_err();

    assert!(res);
    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(peer2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}
