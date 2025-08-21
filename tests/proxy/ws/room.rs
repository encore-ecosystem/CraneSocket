use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};
use upnpsocket::{
    server::{WebSocketProxyServer, message::ServerMessage},
    socket::proxy::WebSocketConnection,
};

use crate::{MPSC_CHANNEL_CAPACITY, TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_create_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let client2room = server.get_client2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (mut first_conn, _) = timed(WebSocketConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    let (mut second_conn, _) = timed(WebSocketConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 2);
    assert_eq!(client2room.read().await.len(), 2);

    timed(first_conn.close(None)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    timed(second_conn.close(None)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_create_and_join_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let client2room = server.get_client2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (mut create_room_conn, room_id) = timed(WebSocketConnection::create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    let mut join_room_conn = timed(WebSocketConnection::join_room(&server_addr, room_id))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 2);

    let msg = timed(create_room_conn.next()).await.unwrap();

    assert_eq!(msg, ServerMessage::ClientJoined);

    timed(join_room_conn.close(None)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    timed(create_room_conn.close(None)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_join_nonexisting_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(WebSocketProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let rooms = server.get_rooms();
    let client2room = server.get_client2room();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    let room_id = "123".to_string();
    let res = timed(WebSocketConnection::join_room(&server_addr, room_id))
        .await
        .is_err();

    assert!(res);
    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    let room_id = "".to_string();
    let res = timed(WebSocketConnection::join_room(&server_addr, room_id))
        .await
        .is_err();

    assert!(res);
    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_early_server_shutdown() {
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let (mut ws_stream, room_id) = timed(WebSocketConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(ws_stream.wait_for_client()).await.unwrap();
        let msg = timed(ws_stream.next()).await.unwrap();
        timed(tx1.send(msg)).await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut ws_stream = timed(WebSocketConnection::join_room(&server_addr, room_id))
            .await
            .unwrap();
        let msg = timed(ws_stream.next()).await.unwrap();
        timed(tx2.send(msg)).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;
    shutdown_tx.send(()).unwrap();

    let received_msg = timed(rx1.recv()).await.expect("No message received");
    assert_eq!(received_msg, ServerMessage::ServerClosed);
    let received_msg = timed(rx2.recv()).await.expect("No message received");
    assert_eq!(received_msg, ServerMessage::ServerClosed);

    sender_handle.abort();
    receiver_handle.abort();
}
