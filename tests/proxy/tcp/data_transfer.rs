use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use tokio_tungstenite::tungstenite::Bytes;
use upnpsocket::{
    server::{
        TcpProxyServer,
        message::{ClientMessage, ServerMessage},
    },
    socket::proxy::TcpConnection,
};

use crate::{TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_text_data_transfer_success() {
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(1);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(1);

    let (mut ws_stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(ws_stream.wait_for_client()).await.unwrap();
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();

        let text = "Hello from receiver".to_string();
        timed(ws_stream.send_all(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut ws_stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .unwrap();
        let text = "Hello from sender".to_string();
        timed(ws_stream.send_all(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg)).await.unwrap();
    });

    let received_msg = timed(rx1.recv()).await.expect("No message received");
    assert_eq!(
        received_msg,
        ServerMessage::Text("Hello from sender".into())
    );
    let received_msg = timed(rx2.recv()).await.expect("No message received");
    assert_eq!(
        received_msg,
        ServerMessage::Text("Hello from receiver".into())
    );

    sender_handle.abort();
    let received_msg = timed(rx1.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);

    receiver_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_text_data_transfer_alone_in_room() {
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

    let (mut ws_stream, _) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();

    let test_message = "Hello".to_string();
    let res =
        timed(ws_stream.send_all(&ClientMessage::text(test_message.clone()).as_bytes())).await;
    assert!(res.is_ok());

    timed(ws_stream.close()).await.unwrap();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_binary_data_transfer_success() {
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(1);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(1);

    let (mut ws_stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(ws_stream.wait_for_client()).await.unwrap();
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");
        let binary_message = Bytes::copy_from_slice("Hello1".as_bytes());
        timed(ws_stream.send_all(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
    });

    let sender_handle = tokio::spawn(async move {
        let mut ws_stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");

        let binary_message = Bytes::copy_from_slice("Hello2".as_bytes());
        timed(ws_stream.send_all(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
        let data = timed(ws_stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let received_msg = timed(rx1.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::Binary("Hello2".into()));
    let received_msg = timed(rx2.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::Binary("Hello1".into()));

    receiver_handle.abort();
    let received_msg = timed(rx2.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);

    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_invalid_message_transfer() {
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(1);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(1);

    let (mut ws_stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(ws_stream.wait_for_client()).await.unwrap();
        let data = timed(ws_stream.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let sender_handle = tokio::spawn(async move {
        let mut ws_stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");
        let binary_message = vec![1, 2, 3];
        timed(ws_stream.send_all(&binary_message)) // sending invalid message
            .await
            .expect("Failed to send message");
        let data = timed(ws_stream.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let received_msg = timed(rx1.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);
    let received_msg = timed(rx2.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::Error("Invalid message".into()));

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_empty_message_transfer() {
    dotenv::dotenv().ok();
    env_logger::init();
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

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(1);

    let (mut ws_stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(ws_stream.wait_for_client()).await.unwrap();
        let data = timed(ws_stream.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let sender_handle = tokio::spawn(async move {
        let mut ws_stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");
        let binary_message = vec![];
        timed(ws_stream.send_all(&binary_message)) // sending empty message
            .await
            .expect("Failed to send message");
    });

    let received_msg = timed(rx.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}
