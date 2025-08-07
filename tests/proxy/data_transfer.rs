use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use tokio_tungstenite::tungstenite::Bytes;
use upnpsocket::{
    server::{
        ProxyServer,
        message::{ClientMessage, ServerMessage},
    },
    socket::proxy::WebSocketConnection,
};

use crate::{TEST_SLEEP_TIME_MS, TEST_TIMEOUT_TIME_MS};

#[tokio::test]
async fn test_text_data_transfer_success() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = ProxyServer::bind(&bind_addr, false).await.unwrap();
    let server_addr = server.get_local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (tx, mut rx) = mpsc::channel::<String>(100);

    let (mut ws_stream, room_id) = WebSocketConnection::create_room(&server_addr)
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        ws_stream.wait_for_client().await.unwrap();
        while let Ok(ServerMessage::Text(msg)) = ws_stream.next().await {
            tx.send(msg.to_string())
                .await
                .expect("Failed to send to channel");
        }
    });

    let sender_handle = tokio::spawn(async move {
        let ws_stream = WebSocketConnection::join_room(&server_addr, room_id)
            .await
            .expect("Failed to connect sender");
        let (mut write, _) = ws_stream.split();

        let test_message = "Hello".to_string();
        println!("going to send msg");
        write
            .send(ClientMessage::text(test_message.clone()))
            .await
            .expect("Failed to send message");

        println!("sent msg");
    });

    let received_msg = tokio::time::timeout(
        tokio::time::Duration::from_secs(TEST_TIMEOUT_TIME_MS),
        rx.recv(),
    )
    .await
    .expect("Timeout waiting for message")
    .expect("No message received");

    assert_eq!(received_msg, "Hello");

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_text_data_transfer_alone_in_room() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = ProxyServer::bind(&bind_addr, false).await.unwrap();
    let server_addr = server.get_local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (mut ws_stream, _) = WebSocketConnection::create_room(&server_addr)
        .await
        .unwrap();

    let test_message = "Hello".to_string();
    let res = ws_stream
        .send(ClientMessage::text(test_message.clone()))
        .await;

    assert!(res.is_ok());

    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_binary_data_transfer_success() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = ProxyServer::bind(&bind_addr, false).await.unwrap();
    let server_addr = server.get_local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (tx, mut rx) = mpsc::channel::<Bytes>(100);

    let (mut ws_stream, room_id) = WebSocketConnection::create_room(&server_addr)
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        ws_stream.wait_for_client().await.unwrap();
        while let Ok(ServerMessage::Binary(msg)) = ws_stream.next().await {
            tx.send(msg).await.expect("Failed to send to channel");
        }
    });

    let sender_handle = tokio::spawn(async move {
        let ws_stream = WebSocketConnection::join_room(&server_addr, room_id)
            .await
            .expect("Failed to connect sender");
        let (mut write, _) = ws_stream.split();

        let binary_message = Bytes::copy_from_slice("Hello".as_bytes());
        write
            .send(ClientMessage::binary(binary_message.clone()))
            .await
            .expect("Failed to send message");
    });

    let received_msg = tokio::time::timeout(
        tokio::time::Duration::from_secs(TEST_TIMEOUT_TIME_MS),
        rx.recv(),
    )
    .await
    .expect("Timeout waiting for message")
    .expect("No message received");

    assert_eq!(received_msg, Bytes::copy_from_slice("Hello".as_bytes()));

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}
