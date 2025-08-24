use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Bytes;
use upnpsocket::{
    server::{
        UdpProxyServer,
        message::{ClientMessage, ServerMessage},
    },
    socket::proxy::UdpConnection,
};

use crate::{MPSC_CHANNEL_CAPACITY, TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_create_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
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

    let mut first_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    timed(first_conn.create_room(&server_addr)).await.unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    let mut second_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    timed(second_conn.create_room(&server_addr)).await.unwrap();

    assert_eq!(rooms.read().await.len(), 2);
    assert_eq!(client2room.read().await.len(), 2);

    timed(first_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    timed(second_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_create_and_join_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
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

    let mut create_room_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let room_id = timed(create_room_conn.create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    let mut join_room_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    timed(join_room_conn.join_room(&server_addr, room_id))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 2);

    let msg = timed(create_room_conn.next()).await.unwrap();
    assert_eq!(msg, ServerMessage::ClientJoined.as_bytes());

    timed(join_room_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    timed(create_room_conn.close()).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_join_nonexisting_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
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
    let mut join_room_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let res = timed(join_room_conn.join_room(&server_addr, room_id))
        .await
        .is_err();

    assert!(res);
    assert_eq!(rooms.read().await.len(), 0);
    assert_eq!(client2room.read().await.len(), 0);

    let room_id = "".to_string();
    let mut join_room_conn = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let res = timed(join_room_conn.join_room(&server_addr, room_id))
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

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
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

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let room_id = timed(socket.create_room(&server_addr)).await.unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(socket.wait_for_client()).await.unwrap();
        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();
        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
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

#[tokio::test]
async fn test_ping() {
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

    let frame = Bytes::from("123");

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    socket.as_raw_socket().connect(server_addr).await.unwrap();

    socket
        .send(&ClientMessage::Ping(frame.clone()).as_bytes())
        .await
        .unwrap();

    let data = timed(socket.next()).await.unwrap();
    let received_msg = ServerMessage::try_from(&data[..]).unwrap();

    assert_eq!(received_msg, ServerMessage::Pong(frame));

    shutdown_tx.send(()).unwrap();
    socket.close().await.unwrap();
}

#[tokio::test]
async fn test_send_unexpected_message() {
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

    let frame = Bytes::from("123");

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    socket.as_raw_socket().connect(server_addr).await.unwrap();

    socket
        .send(&ClientMessage::Pong(frame.clone()).as_bytes())
        .await
        .unwrap();

    let data = timed(socket.next()).await.unwrap();
    let received_msg = ServerMessage::try_from(&data[..]).unwrap();

    match received_msg {
        ServerMessage::Error(msg) => {
            assert!(msg.contains("Received unexpected message: Pong"));
        }
        _ => panic!("Expected ServerMessage::Error, got {:?}", received_msg),
    }

    shutdown_tx.send(()).unwrap();
    socket.close().await.unwrap();
}
