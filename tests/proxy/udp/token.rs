use std::net::SocketAddr;

use tokio::sync::oneshot;

use crane_socket::{
    server::{UdpProxyServer, message::ServerMessage},
    socket::proxy::UdpConnection,
};

use crate::{TEST_SLEEP_TIME_MS, timed};

#[tokio::test]
async fn test_create_and_join_room_using_token() {
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

    let mut create_room_conn = UdpConnection::bind(&bind_addr).await.unwrap();
    let _ = timed(create_room_conn.create_room(&server_addr))
        .await
        .unwrap();

    assert_eq!(rooms.read().await.len(), 1);
    assert_eq!(client2room.read().await.len(), 1);

    let token = create_room_conn.get_token().unwrap();

    let mut join_room_conn = UdpConnection::bind(&bind_addr).await.unwrap();
    timed(join_room_conn.from_token(&token)).await.unwrap();

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
