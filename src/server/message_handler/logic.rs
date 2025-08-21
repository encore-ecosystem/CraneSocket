use log::debug;
use log::error;
use log::info;
use rand::distr::Alphanumeric;
use rand::distr::SampleString;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;

use crate::server::Room;
use crate::server::constant::MAX_NUM_CLIENTS_IN_ROOM;
use crate::server::constant::MAX_NUM_ROOMS;
use crate::server::error::ProxyServerError;
use crate::server::message::ServerMessage;

pub async fn handle_create_room(
    addr: SocketAddr,
    tx: mpsc::Sender<(ServerMessage, SocketAddr)>,
    rooms: &mut HashMap<String, Room>,
    client2room: &mut HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    if rooms.len() > MAX_NUM_ROOMS {
        send_error(&tx, &addr, "No available rooms left").await?;
        return Ok(());
    };

    if let Some(room_id) = client2room.get(&addr) {
        send_error(&tx, &addr, format!("Peer is already in a room {}", room_id)).await?;
        return Ok(());
    }

    let room_id = loop {
        let id = Alphanumeric.sample_string(&mut rand::rng(), 8);
        if !rooms.contains_key(&id) {
            break id;
        }
    };

    let mut room = HashMap::new();
    room.insert(addr, tx.clone());
    rooms.insert(room_id.clone(), room);
    client2room.insert(addr, room_id.clone());

    let response = ServerMessage::RoomCreated(room_id.clone());
    tx.send((response, addr))
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    info!("Created room {} requested by peer={}", room_id, addr);
    Ok(())
}

pub async fn handle_join_room(
    addr: SocketAddr,
    room_id: String,
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    rooms: &mut HashMap<String, Room>,
    client2room: &mut HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    if client2room.contains_key(&addr) {
        send_error(tx, &addr, "Client is already in a room").await?;
        return Ok(());
    }

    let room = match rooms.get_mut(&room_id) {
        Some(room) => room,
        None => {
            send_error(
                tx,
                &addr,
                format!("No room with room_id={} exists", room_id),
            )
            .await?;
            return Ok(());
        }
    };

    if room.len() > MAX_NUM_CLIENTS_IN_ROOM {
        send_error(tx, &addr, "The room is full").await?;
        return Ok(());
    };

    room.insert(addr, tx.clone());
    client2room.insert(addr, room_id.clone());
    info!("Peer {} joined room {}", addr, room_id);

    let response = ServerMessage::JoinedSuccessfully;
    tx.send((response, addr))
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;

    for (other_client_addr, sender) in room.iter() {
        if *other_client_addr != addr {
            log::debug!("Sending ClientJoined msg to {}", *other_client_addr);
            let response = ServerMessage::ClientJoined;
            sender
                .send((response, *other_client_addr))
                .await
                .map_err(|e| {
                    error!("Failed to notify peer {}: {}", other_client_addr, e);
                    ProxyServerError::Send(e.to_string())
                })?;
        }
    }
    Ok(())
}

pub async fn handle_leave_room(
    addr: SocketAddr,
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    rooms: &mut HashMap<String, Room>,
    client2room: &mut HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match client2room.remove(&addr) {
        Some(room_id) => room_id,
        None => {
            send_error(tx, &addr, "Peer is not in a room").await?;
            return Ok(());
        }
    };

    let room = match rooms.get_mut(&room_id) {
        Some(room) => room,
        None => {
            send_error(
                tx,
                &addr,
                format!("No room with room_id={} exists", room_id),
            )
            .await?;
            return Ok(());
        }
    };

    if room.remove(&addr).is_none() {
        send_error(
            tx,
            &addr,
            format!("Peer {} not found in room {}", addr, room_id),
        )
        .await?;
        return Ok(());
    }
    info!("Peer {} left room {}", addr, room_id);

    for (other_client_addr, sender) in room.iter() {
        let msg = ServerMessage::ClientLeft;
        sender.send((msg, *other_client_addr)).await.map_err(|e| {
            error!("Failed to notify peer {}: {}", other_client_addr, e);
            ProxyServerError::Send(e.to_string())
        })?;
    }

    if room.is_empty() {
        rooms.remove(&room_id);
        info!("Room {} deleted", room_id);
    }
    Ok(())
}

pub async fn handle_binary_data(
    addr: SocketAddr,
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    data: Vec<u8>,
    rooms: &HashMap<String, Room>,
    client2room: &HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match client2room.get(&addr) {
        Some(room) => room,
        None => {
            send_error(tx, &addr, format!("No room found for peer {}", addr)).await?;
            return Ok(());
        }
    };

    let room = match rooms.get(room_id) {
        Some(room) => room,
        None => {
            send_error(
                tx,
                &addr,
                format!("No room with room_id={} exists", room_id),
            )
            .await?;
            return Ok(());
        }
    };

    for (other_client_addr, sender) in room.iter() {
        if *other_client_addr != addr {
            debug!(
                "Forwarding binary message from {} to {}",
                addr, other_client_addr
            );
            sender
                .send((ServerMessage::binary(data.clone()), *other_client_addr))
                .await
                .map_err(|e| {
                    error!("Failed to send message to {}: {}", other_client_addr, e);
                    ProxyServerError::Send(e.to_string())
                })?;
        }
    }
    Ok(())
}

pub async fn handle_text_data(
    addr: SocketAddr,
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    data: String,
    rooms: &HashMap<String, Room>,
    client2room: &HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match client2room.get(&addr) {
        Some(room) => room,
        None => {
            send_error(tx, &addr, format!("No room found for peer {}", addr)).await?;
            return Ok(());
        }
    };

    let room = match rooms.get(room_id) {
        Some(room) => room,
        None => {
            send_error(
                tx,
                &addr,
                format!("No room with room_id={} exists", room_id),
            )
            .await?;
            return Ok(());
        }
    };

    let response = ServerMessage::text(data);

    for (other_client_addr, sender) in room.iter() {
        if *other_client_addr != addr {
            debug!(
                "Forwarding text message from {} to {}",
                addr, other_client_addr
            );
            sender
                .send((response.clone(), *other_client_addr))
                .await
                .map_err(|e| {
                    error!("Failed to send message to {}: {}", other_client_addr, e);
                    ProxyServerError::Send(e.to_string())
                })?;
        }
    }
    Ok(())
}

pub async fn send_error(
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    addr: &SocketAddr,
    error_msg: impl Into<String>,
) -> Result<(), ProxyServerError> {
    let msg = ServerMessage::Error(error_msg.into());
    tx.send((msg, *addr))
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    Ok(())
}

pub async fn close_server(
    tx: &mpsc::Sender<(ServerMessage, SocketAddr)>,
    client2room: &mut HashMap<SocketAddr, String>,
) -> Result<(), ProxyServerError> {
    let msg = ServerMessage::ServerClosed;
    for client_addr in client2room.keys() {
        tx.send((msg.clone(), *client_addr))
            .await
            .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    }

    Ok(())
}

#[allow(unused_imports)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_create_room_success() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();

        let result = handle_create_room(addr, tx, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 1);
        assert_eq!(client2room.len(), 1);

        let room_id = client2room.get(&addr).unwrap();
        assert!(rooms.contains_key(room_id));
        assert_eq!(rooms.get(room_id).unwrap().len(), 1);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, (ServerMessage::RoomCreated(_), _addr)));
    }

    #[tokio::test]
    async fn test_create_room_already_in_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        client2room.insert(addr, "room1".to_string());

        let result = handle_create_room(addr, tx, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, (ServerMessage::Error(_), _addr)));
        if let (ServerMessage::Error(e), _addr) = msg {
            assert!(e.contains("Peer is already in a room"));
            assert_eq!(addr, _addr)
        }
    }

    #[tokio::test]
    async fn test_join_room_success() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr1 = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let addr2 = SocketAddr::from_str("1.2.3.4:12345").unwrap();
        let room_id = "room1";

        let mut room = HashMap::new();
        room.insert(addr1, tx1.clone());
        rooms.insert(room_id.to_string(), room);
        client2room.insert(addr1, room_id.to_string());

        let result = handle_join_room(
            addr2,
            room_id.to_string(),
            &tx2,
            &mut rooms,
            &mut client2room,
        )
        .await;
        assert!(result.is_ok());

        let room = rooms.get(room_id).unwrap();
        assert_eq!(room.len(), 2);
        assert!(room.contains_key(&addr2));
        assert_eq!(client2room.get(&addr2), Some(&room_id.to_string()));

        let msg1 = rx2.recv().await.unwrap();
        assert_eq!(msg1, (ServerMessage::JoinedSuccessfully, addr2));

        let msg2 = rx1.recv().await.unwrap();
        assert_eq!(msg2, (ServerMessage::ClientJoined, addr1));
    }

    #[tokio::test]
    async fn test_join_same_peer_twice() {
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let room_id = "room1".to_string();

        let mut room = HashMap::new();
        room.insert(addr, tx1.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr, room_id.clone());

        let res = handle_join_room(addr, room_id, &tx2, &mut rooms, &mut client2room).await;
        assert!(res.is_ok());

        let (msg, recv_addr) = rx2.recv().await.unwrap();
        assert_eq!(recv_addr, addr);
        if let ServerMessage::Error(e) = msg {
            assert!(e.contains("Client is already in a room"));
        } else {
            panic!("Expected error message");
        }
    }

    #[tokio::test]
    async fn test_join_nonexistent_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let room_id = "room1".to_string();

        let result = handle_join_room(addr, room_id, &tx, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);
        assert_eq!(client2room.len(), 0);

        let (msg, recv_addr) = rx.recv().await.unwrap();
        assert_eq!(recv_addr, addr);
        if let ServerMessage::Error(e) = msg {
            assert!(e.contains("No room with room_id"));
        } else {
            panic!("Expected error message");
        }
    }

    #[tokio::test]
    async fn test_leave_room_success() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr1 = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let addr2 = SocketAddr::from_str("1.2.3.4:1235").unwrap();
        let room_id = "room1".to_string();

        let mut room = HashMap::new();
        room.insert(addr1, tx1.clone());
        room.insert(addr2, tx2.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr1, room_id.clone());
        client2room.insert(addr2, room_id.clone());

        let result = handle_leave_room(addr2, &tx2, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        let room = rooms.get(&room_id).unwrap();
        assert_eq!(room.len(), 1);
        assert!(!room.contains_key(&addr2));
        assert!(!client2room.contains_key(&addr2));

        let (msg, recv_addr) = rx1.recv().await.unwrap();
        assert_eq!(recv_addr, addr1);
        assert_eq!(msg, ServerMessage::ClientLeft);
    }

    #[tokio::test]
    async fn test_leave_room_empty_room_deleted() {
        let (tx, _rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let room_id = "room1".to_string();

        let mut room = HashMap::new();
        room.insert(addr, tx.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr, room_id.clone());

        let result = handle_leave_room(addr, &tx, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);
        assert_eq!(client2room.len(), 0);
    }

    #[tokio::test]
    async fn test_leave_nonexistent_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();

        let result = handle_leave_room(addr, &tx, &mut rooms, &mut client2room).await;
        assert!(result.is_ok());

        let (msg, recv_addr) = rx.recv().await.unwrap();
        assert_eq!(recv_addr, addr);
        assert_eq!(
            msg,
            ServerMessage::Error("Peer is not in a room".to_string())
        );
    }

    #[tokio::test]
    async fn test_binary_data_forwarding() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr1 = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let addr2 = SocketAddr::from_str("1.2.3.4:1235").unwrap();
        let room_id = "room1".to_string();
        let data = vec![1, 2, 3];

        let mut room = HashMap::new();
        room.insert(addr1, tx1.clone());
        room.insert(addr2, tx2.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr1, room_id.clone());
        client2room.insert(addr2, room_id.clone());

        let result = handle_binary_data(addr1, &tx1, data.clone(), &rooms, &client2room).await;
        assert!(result.is_ok());

        let (msg, recv_addr) = rx2.recv().await.unwrap();
        assert_eq!(recv_addr, addr2);
        assert_eq!(msg, ServerMessage::binary(data));

        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_text_data_forwarding() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr1 = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let addr2 = SocketAddr::from_str("1.2.3.4:1235").unwrap();
        let room_id = "room1".to_string();
        let text = "Hello".to_string();

        let mut room = HashMap::new();
        room.insert(addr1, tx1.clone());
        room.insert(addr2, tx2.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr1, room_id.clone());
        client2room.insert(addr2, room_id.clone());

        let result = handle_text_data(addr1, &tx1, text.clone(), &rooms, &client2room).await;
        assert!(result.is_ok());

        let (msg, recv_addr) = rx2.recv().await.unwrap();
        assert_eq!(recv_addr, addr2);
        assert_eq!(msg, ServerMessage::text(text));

        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_send_after_leave() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut client2room = HashMap::new();
        let addr1 = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let addr2 = SocketAddr::from_str("1.2.3.4:1235").unwrap();
        let room_id = "room1".to_string();

        let mut room = HashMap::new();
        room.insert(addr1, tx1.clone());
        room.insert(addr2, tx2.clone());
        rooms.insert(room_id.clone(), room);
        client2room.insert(addr1, room_id.clone());
        client2room.insert(addr2, room_id.clone());

        let _ = handle_leave_room(addr2, &tx2, &mut rooms, &mut client2room).await;
        assert!(!client2room.contains_key(&addr2));

        let res = handle_text_data(addr1, &tx1, "Hello".to_string(), &rooms, &client2room).await;
        assert!(res.is_ok());

        assert_eq!(rx1.try_recv().unwrap(), (ServerMessage::ClientLeft, addr1));
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_send_error() {
        let (tx, mut rx) = mpsc::channel(10);
        let addr = SocketAddr::from_str("1.2.3.4:1234").unwrap();
        let error_msg = "Test".to_string();

        let result = send_error(&tx, &addr, error_msg.clone()).await;
        assert!(result.is_ok());

        let (msg, recv_addr) = rx.recv().await.unwrap();
        assert_eq!(recv_addr, addr);
        assert_eq!(msg, ServerMessage::Error(error_msg));
    }
}
