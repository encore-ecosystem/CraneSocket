use log::debug;
use log::error;
use log::info;
use rand::distr::Alphanumeric;
use rand::distr::SampleString;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::server::Room;
use crate::server::error::ProxyServerError;
use crate::server::message::ServerMessage;

pub async fn handle_create_room(
    peer_id: &str,
    tx: mpsc::Sender<ServerMessage>,
    rooms: &mut HashMap<String, Room>,
    peer2room: &mut HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    if let Some(room_id) = peer2room.get(peer_id) {
        send_error(&tx, format!("Peer is already in a room {}", room_id)).await?;
        return Ok(());
    }

    let room_id = loop {
        let id = Alphanumeric.sample_string(&mut rand::rng(), 8);
        if !rooms.contains_key(&id) {
            break id;
        }
    };

    let mut room = HashMap::new();
    room.insert(peer_id.to_string(), tx.clone());
    rooms.insert(room_id.clone(), room);
    peer2room.insert(peer_id.to_string(), room_id.clone());

    let response = ServerMessage::RoomCreated(room_id.clone());
    tx.send(response)
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    info!("Created room {} requested by peer={}", room_id, peer_id);
    Ok(())
}

pub async fn handle_join_room(
    peer_id: &str,
    room_id: String,
    tx: &mpsc::Sender<ServerMessage>,
    rooms: &mut HashMap<String, Room>,
    peer2room: &mut HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    if peer2room.contains_key(peer_id) {
        send_error(tx, "Client is already in a room").await?;
        return Ok(());
    }

    let room = match rooms.get_mut(&room_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room with room_id={} exists", room_id)).await?;
            return Ok(());
        }
    };

    room.insert(peer_id.to_string(), tx.clone());
    peer2room.insert(peer_id.to_string(), room_id.clone());
    info!("Peer {} joined room {}", peer_id, room_id);

    let response = ServerMessage::JoinedSuccessfully;
    tx.send(response)
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;

    for (other_peer_id, sender) in room.iter() {
        if other_peer_id != peer_id {
            let response = ServerMessage::ClientJoined;
            sender.send(response).await.map_err(|e| {
                error!("Failed to notify peer {}: {}", other_peer_id, e);
                ProxyServerError::Send(e.to_string())
            })?;
        }
    }
    Ok(())
}

pub async fn handle_leave_room(
    peer_id: &str,
    tx: &mpsc::Sender<ServerMessage>,
    rooms: &mut HashMap<String, Room>,
    peer2room: &mut HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match peer2room.remove(peer_id) {
        Some(room_id) => room_id,
        None => {
            send_error(tx, "Peer is not in a room").await?;
            return Ok(());
        }
    };

    let room = match rooms.get_mut(&room_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room with room_id={} exists", room_id)).await?;
            return Ok(());
        }
    };

    if room.remove(peer_id).is_none() {
        send_error(
            tx,
            format!("Peer {} not found in room {}", peer_id, room_id),
        )
        .await?;
        return Ok(());
    }
    info!("Peer {} left room {}", peer_id, room_id);

    for (other_peer_id, sender) in room.iter() {
        let msg = ServerMessage::ClientLeft;
        sender.send(msg).await.map_err(|e| {
            error!("Failed to notify peer {}: {}", other_peer_id, e);
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
    peer_id: &str,
    tx: &mpsc::Sender<ServerMessage>,
    data: Vec<u8>,
    rooms: &HashMap<String, Room>,
    peer2room: &HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match peer2room.get(peer_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room found for peer {}", peer_id)).await?;
            return Ok(());
        }
    };

    let room = match rooms.get(room_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room with room_id={} exists", room_id)).await?;
            return Ok(());
        }
    };

    for (other_peer_id, sender) in room.iter() {
        if other_peer_id != peer_id {
            debug!(
                "Forwarding binary message from {} to {}",
                peer_id, other_peer_id
            );
            sender
                .send(ServerMessage::binary(data.clone()))
                .await
                .map_err(|e| {
                    error!("Failed to send message to {}: {}", other_peer_id, e);
                    ProxyServerError::Send(e.to_string())
                })?;
        }
    }
    Ok(())
}

pub async fn handle_text_data(
    peer_id: &str,
    tx: &mpsc::Sender<ServerMessage>,
    data: String,
    rooms: &HashMap<String, Room>,
    peer2room: &HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = match peer2room.get(peer_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room found for peer {}", peer_id)).await?;
            return Ok(());
        }
    };

    let room = match rooms.get(room_id) {
        Some(room) => room,
        None => {
            send_error(tx, format!("No room with room_id={} exists", room_id)).await?;
            return Ok(());
        }
    };

    let response = ServerMessage::text(data);

    for (other_peer_id, sender) in room.iter() {
        if other_peer_id != peer_id {
            debug!(
                "Forwarding text message from {} to {}",
                peer_id, other_peer_id
            );
            sender.send(response.clone()).await.map_err(|e| {
                error!("Failed to send message to {}: {}", other_peer_id, e);
                ProxyServerError::Send(e.to_string())
            })?;
        }
    }
    Ok(())
}

pub async fn send_error(
    tx: &mpsc::Sender<ServerMessage>,
    error_msg: impl Into<String>,
) -> Result<(), ProxyServerError> {
    let response = ServerMessage::Error(error_msg.into());
    tx.send(response)
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    Ok(())
}

#[allow(unused_imports)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_room_success() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1";

        let result = handle_create_room(peer_id, tx, &mut rooms, &mut peer2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 1);
        assert_eq!(peer2room.len(), 1);

        let room_id = peer2room.get(peer_id).unwrap();
        assert!(rooms.contains_key(room_id));
        assert_eq!(rooms.get(room_id).unwrap().len(), 1);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, ServerMessage::RoomCreated(_)));
    }

    #[tokio::test]
    async fn test_create_room_already_in_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1";
        peer2room.insert(peer_id.to_string(), "room1".to_string());

        let result = handle_create_room(peer_id, tx, &mut rooms, &mut peer2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, ServerMessage::Error(_)));
        if let ServerMessage::Error(e) = msg {
            assert!(e.contains("Peer is already in a room"));
        }
    }

    #[tokio::test]
    async fn test_join_room_success() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id1 = "peer1";
        let peer_id2 = "peer2";
        let room_id = "room1";

        let mut room = HashMap::new();
        room.insert(peer_id1.to_string(), tx1.clone());
        rooms.insert(room_id.to_string(), room);
        peer2room.insert(peer_id1.to_string(), room_id.to_string());

        let result = handle_join_room(
            peer_id2,
            room_id.to_string(),
            &tx2,
            &mut rooms,
            &mut peer2room,
        )
        .await;
        assert!(result.is_ok());

        let room = rooms.get(room_id).unwrap();
        assert_eq!(room.len(), 2);
        assert!(room.contains_key(peer_id2));
        assert_eq!(peer2room.get(peer_id2), Some(&room_id.to_string()));

        let msg1 = rx2.recv().await.unwrap();
        assert_eq!(msg1, ServerMessage::JoinedSuccessfully);

        let msg2 = rx1.recv().await.unwrap();
        assert_eq!(msg2, ServerMessage::ClientJoined);
    }

    #[tokio::test]
    async fn test_join_same_peer_twice() {
        let (tx1, mut _rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1".to_string();
        let room_id = "room1".to_string();

        let mut room = HashMap::new();
        room.insert(peer_id.clone(), tx1.clone());
        rooms.insert(room_id.clone(), room);
        peer2room.insert(peer_id.clone(), room_id.clone());

        let res = handle_join_room(&peer_id, room_id, &tx2, &mut rooms, &mut peer2room).await;
        assert!(res.is_ok());

        let err_msg = rx2.recv().await.unwrap();
        if let ServerMessage::Error(e) = err_msg {
            assert!(e.contains("Client is already in a room"));
        } else {
            panic!("Expected error message");
        }
    }

    #[tokio::test]
    async fn test_join_nonexistent_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1";
        let room_id = "room1";

        let result = handle_join_room(
            peer_id,
            room_id.to_string(),
            &tx,
            &mut rooms,
            &mut peer2room,
        )
        .await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);
        assert_eq!(peer2room.len(), 0);

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, ServerMessage::Error(_)));
        if let ServerMessage::Error(e) = msg {
            assert!(e.contains("No room with room_id"));
        }
    }

    #[tokio::test]
    async fn test_leave_room_success() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id1 = "peer1";
        let peer_id2 = "peer2";
        let room_id = "room1";

        let mut room = HashMap::new();
        room.insert(peer_id1.to_string(), tx1.clone());
        room.insert(peer_id2.to_string(), tx2.clone());
        rooms.insert(room_id.to_string(), room);
        peer2room.insert(peer_id1.to_string(), room_id.to_string());
        peer2room.insert(peer_id2.to_string(), room_id.to_string());

        let result = handle_leave_room(peer_id2, &tx2, &mut rooms, &mut peer2room).await;
        assert!(result.is_ok());

        let room = rooms.get(room_id).unwrap();
        assert_eq!(room.len(), 1);
        assert!(!room.contains_key(peer_id2));
        assert!(!peer2room.contains_key(peer_id2));

        let msg = rx1.recv().await.unwrap();
        assert_eq!(msg, ServerMessage::ClientLeft);
    }

    #[tokio::test]
    async fn test_leave_room_empty_room_deleted() {
        let (tx, _rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1";
        let room_id = "room1";

        let mut room = HashMap::new();
        room.insert(peer_id.to_string(), tx.clone());
        rooms.insert(room_id.to_string(), room);
        peer2room.insert(peer_id.to_string(), room_id.to_string());

        let result = handle_leave_room(peer_id, &tx, &mut rooms, &mut peer2room).await;
        assert!(result.is_ok());

        assert_eq!(rooms.len(), 0);
        assert_eq!(peer2room.len(), 0);
    }

    #[tokio::test]
    async fn test_leave_nonexistent_room() {
        let (tx, mut rx) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id = "peer1";

        let result = handle_leave_room(peer_id, &tx, &mut rooms, &mut peer2room).await;
        assert!(result.is_ok());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, ServerMessage::Error("Peer is not in a room".into()));
    }

    #[tokio::test]
    async fn test_binary_data_forwarding() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id1 = "peer1";
        let peer_id2 = "peer2";
        let room_id = "room1";
        let data = vec![1, 2, 3];

        let mut room = HashMap::new();
        room.insert(peer_id1.to_string(), tx1.clone());
        room.insert(peer_id2.to_string(), tx2.clone());
        rooms.insert(room_id.to_string(), room);
        peer2room.insert(peer_id1.to_string(), room_id.to_string());
        peer2room.insert(peer_id2.to_string(), room_id.to_string());

        let result = handle_binary_data(peer_id1, &tx1, data.clone(), &rooms, &peer2room).await;
        assert!(result.is_ok());

        let msg = rx2.recv().await.unwrap();
        assert_eq!(msg, ServerMessage::binary(data));

        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_text_data_forwarding() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let peer_id1 = "peer1";
        let peer_id2 = "peer2";
        let room_id = "room1";
        let text = "Hello".to_string();

        let mut room = HashMap::new();
        room.insert(peer_id1.to_string(), tx1.clone());
        room.insert(peer_id2.to_string(), tx2.clone());
        rooms.insert(room_id.to_string(), room);
        peer2room.insert(peer_id1.to_string(), room_id.to_string());
        peer2room.insert(peer_id2.to_string(), room_id.to_string());

        let result = handle_text_data(peer_id1, &tx1, text.clone(), &rooms, &peer2room).await;
        assert!(result.is_ok());

        let msg = rx2.recv().await.unwrap();
        assert_eq!(msg, ServerMessage::text(text));

        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_send_after_leave() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let mut rooms = HashMap::new();
        let mut peer2room = HashMap::new();
        let room_id = "room1".to_string();
        let peer_id1 = "peer1".to_string();
        let peer_id2 = "peer2".to_string();

        let mut room = HashMap::new();
        room.insert(peer_id1.clone(), tx1.clone());
        room.insert(peer_id2.clone(), tx2.clone());
        rooms.insert(room_id.clone(), room);
        peer2room.insert(peer_id1.clone(), room_id.clone());
        peer2room.insert(peer_id2.clone(), room_id.clone());

        let _ = handle_leave_room(&peer_id2, &tx2, &mut rooms, &mut peer2room).await;
        assert!(!peer2room.contains_key(&peer_id2));

        let res = handle_text_data(&peer_id1, &tx1, "Hello".to_string(), &rooms, &peer2room).await;
        assert!(res.is_ok());
        assert_eq!(rx1.try_recv().unwrap(), ServerMessage::ClientLeft);
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_send_error() {
        let (tx, mut rx) = mpsc::channel(10);
        let error_msg = "Test".to_string();

        let result = send_error(&tx, error_msg.clone()).await;
        assert!(result.is_ok());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, ServerMessage::Error(error_msg));
    }
}
