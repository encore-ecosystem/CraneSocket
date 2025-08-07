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

    let room = rooms.get_mut(&room_id).ok_or_else(|| {
        debug!("Room {} is not found", room_id);
        ProxyServerError::NotFound(format!("Room {} not found", room_id))
    })?;

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
    data: Vec<u8>,
    rooms: &HashMap<String, Room>,
    peer2room: &HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = peer2room.get(peer_id).ok_or_else(|| {
        ProxyServerError::InvalidData(format!("No room found for peer {}", peer_id))
    })?;

    let room = rooms
        .get(room_id)
        .ok_or_else(|| ProxyServerError::InvalidData(format!("Room {} not found", room_id)))?;

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
    data: String,
    rooms: &HashMap<String, Room>,
    peer2room: &HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = peer2room
        .get(peer_id)
        .ok_or_else(|| ProxyServerError::NotFound(format!("No room found for peer {}", peer_id)))?;

    let room = rooms
        .get(room_id)
        .ok_or_else(|| ProxyServerError::NotFound(format!("Room {} not found", room_id)))?;

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
