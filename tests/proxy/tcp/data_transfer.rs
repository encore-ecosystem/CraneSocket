use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use tokio_tungstenite::tungstenite::Bytes;
use crane_socket::{
    server::{
        TcpProxyServer,
        message::{ClientMessage, ServerMessage},
    },
    socket::proxy::TcpConnection,
};

use crate::{CHUNK_SIZE, FILE_SIZE, MPSC_CHANNEL_CAPACITY, TEST_SLEEP_TIME_MS, timed};

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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let (mut stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(stream.wait_for_client()).await.unwrap();
        let data = timed(stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();

        let text = "Hello from receiver".to_string();
        timed(stream.send_all(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .unwrap();
        let text = "Hello from sender".to_string();
        timed(stream.send_all(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(stream.next()).await.unwrap();
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

    let (mut stream, _) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();

    let test_message = "Hello".to_string();
    let res = timed(stream.send_all(&ClientMessage::text(test_message.clone()).as_bytes())).await;
    assert!(res.is_ok());

    timed(stream.close()).await.unwrap();
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let (mut stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(stream.wait_for_client()).await.unwrap();
        let data = timed(stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");
        let binary_message = Bytes::copy_from_slice("Hello1".as_bytes());
        timed(stream.send_all(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
    });

    let sender_handle = tokio::spawn(async move {
        let mut stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");

        let binary_message = Bytes::copy_from_slice("Hello2".as_bytes());
        timed(stream.send_all(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(stream.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
        let data = timed(stream.next()).await.unwrap();
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let (mut stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(stream.wait_for_client()).await.unwrap();
        let data = timed(stream.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let sender_handle = tokio::spawn(async move {
        let mut stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");
        let binary_message = vec![1, 2, 3];
        timed(stream.send_all(&binary_message)) // sending invalid message
            .await
            .expect("Failed to send message");
        let data = timed(stream.next()).await.expect("No msg received");
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

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let (mut stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let receiver_handle = tokio::spawn(async move {
        timed(stream.wait_for_client()).await.unwrap();
        let data = timed(stream.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let sender_handle = tokio::spawn(async move {
        let mut stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");
        let binary_message = vec![];
        timed(stream.send_all(&binary_message)) // sending empty message
            .await
            .expect("Failed to send message");
    });

    let received_msg = timed(rx.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_large_binary_file_transfer_success() {
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

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);

    let large_data = {
        let mut data = vec![0u8; FILE_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        Bytes::from(data)
    };
    let expected_data = large_data.clone();

    let (mut stream, room_id) = timed(TcpConnection::create_room(&server_addr))
        .await
        .unwrap();
    let handle1 = tokio::spawn(async move {
        timed(stream.wait_for_client()).await.unwrap();
        let mut received_data = Vec::new();
        received_data.reserve_exact(FILE_SIZE);
        while received_data.len() < FILE_SIZE {
            let data: Vec<u8> = timed(stream.next()).await.unwrap();
            let msg = ServerMessage::try_from(&data[..]).unwrap();
            match msg {
                ServerMessage::Binary(data) => {
                    received_data.extend_from_slice(&data);
                    timed(tx1.send(ServerMessage::Binary(data)))
                        .await
                        .expect("Failed to send to channel");
                }
                msg => {
                    log::error!("Received invalid msg: {}", msg);
                    break;
                }
            }
        }

        for chunk in received_data.chunks(CHUNK_SIZE) {
            let chunk_bytes = Bytes::copy_from_slice(chunk);
            timed(stream.send_all(&ClientMessage::binary(chunk_bytes).as_bytes()))
                .await
                .expect("Failed to send message");
        }
        timed(stream.flush()).await.unwrap();
    });

    let handle2 = tokio::spawn(async move {
        let mut stream = timed(TcpConnection::join_room(&server_addr, room_id))
            .await
            .expect("Failed to connect sender");
        for chunk in large_data.chunks(CHUNK_SIZE) {
            let chunk_bytes = Bytes::copy_from_slice(chunk);
            timed(stream.send_all(&ClientMessage::binary(chunk_bytes).as_bytes()))
                .await
                .expect("Failed to send message");
        }
        timed(stream.flush()).await.unwrap();

        let mut received_data = Vec::new();
        received_data.reserve_exact(FILE_SIZE);
        while received_data.len() < FILE_SIZE {
            let data = timed(stream.next()).await.unwrap();
            let msg = ServerMessage::try_from(&data[..]).unwrap();
            if let ServerMessage::Binary(data) = msg {
                received_data.extend_from_slice(&data);
                timed(tx2.send(ServerMessage::Binary(data)))
                    .await
                    .expect("Failed to send to channel");
            } else if matches!(msg, ServerMessage::ClientLeft) {
                timed(tx2.send(msg))
                    .await
                    .expect("Failed to send to channel");
                break;
            }
        }
    });

    let mut received_data1 = Vec::new();
    received_data1.reserve_exact(FILE_SIZE);
    while received_data1.len() < FILE_SIZE {
        let msg = timed(rx1.recv()).await.expect("No msg received");
        match msg {
            ServerMessage::Binary(data) => {
                received_data1.extend_from_slice(&data);
            }
            ServerMessage::ClientLeft => break,
            _ => panic!("Unexpected message type"),
        }
    }
    assert_eq!(received_data1.len(), expected_data.len());
    assert_eq!(received_data1, expected_data.as_ref());

    let mut received_data2 = Vec::new();
    received_data2.reserve_exact(FILE_SIZE);
    while received_data2.len() < FILE_SIZE {
        let msg = timed(rx2.recv()).await.expect("No msg received");
        match msg {
            ServerMessage::Binary(data) => {
                received_data2.extend_from_slice(&data);
            }
            ServerMessage::ClientLeft => break,
            _ => panic!("Unexpected message type"),
        }
    }
    assert_eq!(received_data2.len(), expected_data.len());
    assert_eq!(received_data2, expected_data.as_ref());

    handle1.abort();
    handle2.abort();
    shutdown_tx.send(()).unwrap();
}
