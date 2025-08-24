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

use crate::{CHUNK_SIZE, FILE_SIZE, MPSC_CHANNEL_CAPACITY, TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_text_data_transfer_success() {
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

        let text = "Hello from receiver".to_string();
        timed(socket.send(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg)).await.unwrap();
        socket.close().await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();
        let text = "Hello from sender".to_string();
        timed(socket.send(&ClientMessage::text(text.clone()).as_bytes()))
            .await
            .expect("Failed to send message");

        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg)).await.unwrap();
        socket.close().await.unwrap();
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

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let _ = timed(socket.create_room(&server_addr)).await.unwrap();

    let test_message = "Hello".to_string();
    let res = timed(socket.send(&ClientMessage::text(test_message.clone()).as_bytes())).await;
    assert!(res.is_ok());

    timed(socket.close()).await.unwrap();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_binary_data_transfer_success() {
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
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");
        let binary_message = Bytes::copy_from_slice("Hello1".as_bytes());
        timed(socket.send(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
        socket.close().await.unwrap();
    });

    let sender_handle = tokio::spawn(async move {
        let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();

        let binary_message = Bytes::copy_from_slice("Hello2".as_bytes());
        timed(socket.send(&ClientMessage::binary(binary_message).as_bytes()))
            .await
            .expect("Failed to send message");
        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
        let data = timed(socket.next()).await.unwrap();
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
        socket.close().await.unwrap();
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

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let error_counter_per_client = server.get_error_counter_per_client().await;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (tx1, mut rx1) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx3, mut rx3) = mpsc::channel::<bool>(MPSC_CHANNEL_CAPACITY);
    let (tx4, mut rx4) = mpsc::channel::<bool>(MPSC_CHANNEL_CAPACITY);

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let room_id = timed(socket.create_room(&server_addr)).await.unwrap();

    let addr1 = socket.get_local_addr().unwrap();
    assert!(error_counter_per_client.read().await.contains_key(&addr1));
    assert_eq!(error_counter_per_client.read().await.get(&addr1), Some(&0));

    let error_counter_per_client_clone = error_counter_per_client.clone();
    let receiver_handle = tokio::spawn(async move {
        let error_counter_per_client = error_counter_per_client_clone;
        timed(socket.wait_for_client()).await.unwrap();
        let data = timed(socket.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx1.send(msg))
            .await
            .expect("Failed to send to channel");

        let msg = error_counter_per_client.read().await.contains_key(&addr1);
        timed(tx4.send(msg))
            .await
            .expect("Failed to send to channel");
        let msg = error_counter_per_client.read().await.get(&addr1) == Some(&0);
        timed(tx4.send(msg))
            .await
            .expect("Failed to send to channel");

        socket.close().await.unwrap();
    });

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let addr2 = socket.get_local_addr().unwrap();
    let sender_handle = tokio::spawn(async move {
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();

        let msg = error_counter_per_client.read().await.contains_key(&addr2);
        timed(tx3.send(msg))
            .await
            .expect("Failed to send to channel");
        let msg = error_counter_per_client.read().await.get(&addr2) == Some(&0);
        timed(tx3.send(msg))
            .await
            .expect("Failed to send to channel");

        let binary_message = vec![1, 2, 3];
        timed(socket.send(&binary_message)) // sending invalid message
            .await
            .expect("Failed to send message");

        let data = timed(socket.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");

        let msg = error_counter_per_client.read().await.contains_key(&addr2);
        timed(tx3.send(!msg))
            .await
            .expect("Failed to send to channel");
        let msg = error_counter_per_client.read().await.get(&addr2).is_none();
        timed(tx3.send(msg))
            .await
            .expect("Failed to send to channel");

        socket.close().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

        assert!(!error_counter_per_client.read().await.contains_key(&addr2));
    });
    let received_msg = timed(rx1.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);
    let received_msg = timed(rx2.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::Error("Invalid message".into()));

    let contains_key_msg = timed(rx3.recv()).await.expect("No msg received");
    assert!(contains_key_msg);
    let error_count_msg = timed(rx3.recv()).await.expect("No msg received");
    assert!(error_count_msg);

    let contains_key_msg = timed(rx4.recv()).await.expect("No msg received");
    assert!(contains_key_msg);
    let error_count_msg = timed(rx4.recv()).await.expect("No msg received");
    assert!(error_count_msg);

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_empty_message_transfer() {
    let bind_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let server = timed(UdpProxyServer::bind(&bind_addr, false))
        .await
        .unwrap();
    let server_addr = server.get_local_addr().unwrap();
    let error_counter_per_client = server.get_error_counter_per_client().await;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        server.serve(shutdown_rx).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(TEST_SLEEP_TIME_MS)).await;

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(MPSC_CHANNEL_CAPACITY);
    let (tx2, mut rx2) = mpsc::channel::<bool>(MPSC_CHANNEL_CAPACITY);

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let room_id = timed(socket.create_room(&server_addr)).await.unwrap();

    let addr1 = socket.get_local_addr().unwrap();
    assert!(error_counter_per_client.read().await.contains_key(&addr1));
    assert_eq!(error_counter_per_client.read().await.get(&addr1), Some(&0));

    let receiver_handle = tokio::spawn(async move {
        timed(socket.wait_for_client()).await.unwrap();
        let data = timed(socket.next()).await.expect("No msg received");
        let msg = ServerMessage::try_from(&data[..]).unwrap();
        timed(tx.send(msg))
            .await
            .expect("Failed to send to channel");
    });

    let sender_handle = tokio::spawn(async move {
        let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
        let addr2 = socket.get_local_addr().unwrap();
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();
        let binary_message = vec![];
        timed(socket.send(&binary_message)) // sending empty message
            .await
            .expect("Failed to send message");

        let msg = error_counter_per_client.read().await.contains_key(&addr2);
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");
        let msg = error_counter_per_client.read().await.get(&addr2) == Some(&0);
        timed(tx2.send(msg))
            .await
            .expect("Failed to send to channel");

        socket.close().await.unwrap();
    });

    let received_msg = timed(rx.recv()).await.expect("No msg received");
    assert_eq!(received_msg, ServerMessage::ClientLeft);

    let contains_key_msg = timed(rx2.recv()).await.expect("No msg received");
    assert!(contains_key_msg);
    let error_count_msg = timed(rx2.recv()).await.expect("No msg received");
    assert!(error_count_msg);

    receiver_handle.abort();
    sender_handle.abort();
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_large_binary_file_transfer_success() {
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

    let large_data = {
        let mut data = vec![0u8; FILE_SIZE];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        Bytes::from(data)
    };
    let expected_data = large_data.clone();

    let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
    let room_id = timed(socket.create_room(&server_addr)).await.unwrap();
    let handle1 = tokio::spawn(async move {
        timed(socket.wait_for_client()).await.unwrap();
        let mut received_data = Vec::new();
        received_data.reserve_exact(FILE_SIZE);
        while received_data.len() < FILE_SIZE {
            let data: Vec<u8> = timed(socket.next()).await.unwrap();
            let msg = ServerMessage::try_from(&data[..]).unwrap();
            if let ServerMessage::Binary(data) = msg {
                received_data.extend_from_slice(&data);
                timed(tx1.send(ServerMessage::Binary(data)))
                    .await
                    .expect("Failed to send to channel");
            } else if matches!(msg, ServerMessage::ClientLeft) {
                timed(tx1.send(msg))
                    .await
                    .expect("Failed to send to channel");
                break;
            }
        }

        for chunk in received_data.chunks(CHUNK_SIZE) {
            let chunk_bytes = Bytes::copy_from_slice(chunk);
            timed(socket.send(&ClientMessage::binary(chunk_bytes).as_bytes()))
                .await
                .expect("Failed to send message");
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    });

    let handle2 = tokio::spawn(async move {
        let mut socket = timed(UdpConnection::bind(&bind_addr)).await.unwrap();
        timed(socket.join_room(&server_addr, room_id))
            .await
            .unwrap();
        for chunk in large_data.chunks(CHUNK_SIZE) {
            let chunk_bytes = Bytes::copy_from_slice(chunk);
            timed(socket.send(&ClientMessage::binary(chunk_bytes).as_bytes()))
                .await
                .expect("Failed to send message");
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }

        let mut received_data = Vec::new();
        received_data.reserve_exact(FILE_SIZE);
        while received_data.len() < FILE_SIZE {
            let data = timed(socket.next()).await.unwrap();
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
