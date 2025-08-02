use log::debug;
use log::error;
use log::info;
use rand::distr::Alphanumeric;
use rand::distr::SampleString;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::server::Room;
use crate::server::error::ProxyServerError;
use crate::server::message::BinaryMessageTrait;
use crate::server::message::ClientBinaryMessage;
use crate::server::message::ServerTextMessage;
use crate::server::transport::TransportMessage;

pub async fn handle_create_room(
    peer_id: &str,
    tx: mpsc::Sender<TransportMessage>,
    rooms: &mut HashMap<String, Room>,
    peer2room: &mut HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let room_id = Alphanumeric.sample_string(&mut rand::rng(), 8);
    let mut room = HashMap::new();
    room.insert(peer_id.to_string(), tx.clone());
    rooms.insert(room_id.clone(), room);
    peer2room.insert(peer_id.to_string(), room_id.clone());

    let response = ServerTextMessage::RoomCreated {
        room_id: room_id.clone(),
    };
    tx.send(TransportMessage::Text(serde_json::to_string(&response)?))
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    info!("Created room {} requested by peer={}", room_id, peer_id);
    Ok(())
}

pub async fn handle_join_room(
    peer_id: &str,
    room_id: String,
    tx: &mpsc::Sender<TransportMessage>,
    rooms: &mut HashMap<String, Room>,
    peer2room: &mut HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    if let Some(room) = rooms.get_mut(&room_id) {
        if !room.contains_key(peer_id) {
            room.insert(peer_id.to_string(), tx.clone());
            peer2room.insert(peer_id.to_string(), room_id.clone());
            info!("Peer {} joined room {}", peer_id, room_id);

            let response = ServerTextMessage::JoinedSuccessfully;
            tx.send(TransportMessage::Text(serde_json::to_string(&response)?))
                .await
                .map_err(|e| ProxyServerError::Send(e.to_string()))?;

            for (other_peer_id, sender) in room.iter() {
                if other_peer_id != peer_id {
                    let response = ServerTextMessage::ClientJoined;
                    sender
                        .send(TransportMessage::Text(serde_json::to_string(&response)?))
                        .await
                        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
                }
            }
        } else {
            send_error(tx, "Room is full or unavailable").await?;
        }
    } else {
        send_error(tx, "Room not found").await?;
    }
    Ok(())
}

pub async fn handle_leave_room(
    peer_id: &str,
    tx: &mpsc::Sender<TransportMessage>,
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

    if let Some(room) = rooms.get_mut(&room_id) {
        if !room.contains_key(peer_id) {
            send_error(
                tx,
                format!(
                    "Could not delete peer {} from room {}. There is not such peer in this room",
                    peer_id, room_id
                ),
            )
            .await?;
        }
        room.remove(peer_id);
        info!("Peer {} left room {}", peer_id, room_id);

        for (other_peer_id, sender) in room.iter() {
            if other_peer_id != peer_id {
                let msg = ServerTextMessage::ClientLeft;
                sender
                    .send(TransportMessage::Text(serde_json::to_string(&msg)?))
                    .await
                    .map_err(|e| ProxyServerError::Send(e.to_string()))?;
            }
        }

        if room.is_empty() {
            rooms.remove(&room_id);
            info!("Room {} deleted", room_id);
        }
    } else {
        debug!("Room with room_id={} not found", room_id);
        send_error(tx, "Room not found").await?;
    }
    Ok(())
}

pub async fn handle_binary_data(
    peer_id: &str,
    data: Vec<u8>,
    rooms: &HashMap<String, Room>,
    peer2room: &HashMap<String, String>,
) -> Result<(), ProxyServerError> {
    let msg = match ClientBinaryMessage::is_valid(&data) {
        true => match ClientBinaryMessage::decode(&data) {
            Ok(msg) => Ok(msg),
            Err(e) => Err(e),
        },
        false => Err(ProxyServerError::InvalidData(
            "Received invalid binary message".into(),
        )),
    };

    if let Some(room_id) = peer2room.get(peer_id) {
        if let Some(room) = rooms.get(room_id) {
            for (other_peer_id, sender) in room.iter() {
                if other_peer_id != peer_id {
                    match &msg {
                        Ok(binary_msg) => match &binary_msg {
                            ClientBinaryMessage::Data(_) => {
                                debug!(
                                    "Forwarding binary message from {} to {}",
                                    peer_id, other_peer_id
                                );
                                sender
                                    .send(TransportMessage::Binary(data.clone()))
                                    .await
                                    .map_err(|e| ProxyServerError::Send(e.to_string()))?;
                            }
                        },
                        Err(e) => {
                            error!("Failed to decode binary message: {}", e);
                            send_error(sender, "Invalid binary message").await?;
                        }
                    }
                }
            }
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
    if let Some(room_id) = peer2room.get(peer_id) {
        if let Some(room) = rooms.get(room_id) {
            for (other_peer_id, sender) in room.iter() {
                if other_peer_id != peer_id {
                    debug!(
                        "Forwarding text message from {} to {}",
                        peer_id, other_peer_id
                    );
                    let response = ServerTextMessage::Data(data.clone());
                    sender
                        .send(TransportMessage::Text(serde_json::to_string(&response)?))
                        .await
                        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

pub async fn send_error(
    tx: &mpsc::Sender<TransportMessage>,
    error_msg: impl Into<String>,
) -> Result<(), ProxyServerError> {
    let response = ServerTextMessage::Error(error_msg.into());
    let msg = serde_json::to_string(&response).map_err(ProxyServerError::Serialization)?;
    tx.send(TransportMessage::Text(msg))
        .await
        .map_err(|e| ProxyServerError::Send(e.to_string()))?;
    Ok(())
}
