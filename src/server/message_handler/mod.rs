use log::debug;
use log::error;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::server::Peer2Room;
use crate::server::Rooms;
use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::message_handler::handle_binary_data;
use crate::server::message_handler::handle_create_room;
use crate::server::message_handler::handle_join_room;
use crate::server::message_handler::handle_leave_room;
use crate::server::message_handler::handle_text_data;
use crate::server::message_handler::send_error;
use crate::server::transport::Transport;

mod logic;
use logic::*;

pub async fn handle_message<T: Transport>(
    mut transport: T,
    rooms: Rooms,
    peer2room: Peer2Room,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(), ProxyServerError> {
    let peer_id = transport.peer_id().to_string();
    let tx = transport.sender();
    let mut shutdown_rx = shutdown_tx.subscribe();

    loop {
        tokio::select! {
            biased;
            Some(msg_result) = transport.recv() => {
                match msg_result {
                    Ok(msg) => {
                        if let Err(e) = process_message(&peer_id, msg, tx.clone(), &rooms, &peer2room).await {
                            error!("Error processing message for peer {}: {:?}", peer_id, e);
                            send_error(&tx, "Internal error".to_string()).await?;
                        }
                    },
                    Err(e) => {
                        error!("Transport error for peer {}: {:?}", peer_id, e);
                        let _ = shutdown_tx.send(());
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                debug!("Shutdown signal received for peer: {}", peer_id);
                while let Ok(msg) = transport.try_recv() {
                    if let Err(e) = process_message(&peer_id, msg, tx.clone(), &rooms, &peer2room).await {
                        error!("Error flushing message for peer {}: {:?}", peer_id, e);
                    }
                }
                let mut rooms = rooms.write().await;
                let mut peer2room = peer2room.write().await;
                handle_leave_room(&peer_id, &tx, &mut rooms, &mut peer2room).await?;
                break;
            }
        }
    }

    Ok(())
}

async fn process_message(
    peer_id: &str,
    msg: ClientMessage,
    tx: mpsc::Sender<ServerMessage>,
    rooms: &Rooms,
    peer2room: &Peer2Room,
) -> Result<(), ProxyServerError> {
    let mut rooms_lock = rooms.write().await;
    let mut peer2room_lock = peer2room.write().await;
    match msg {
        ClientMessage::Text(data) => {
            drop((rooms_lock, peer2room_lock));
            let rooms = rooms.read().await;
            let peer2room = peer2room.read().await;
            handle_text_data(peer_id, &tx, data.to_string(), &rooms, &peer2room).await?
        }
        ClientMessage::Binary(data) => {
            drop((rooms_lock, peer2room_lock));
            let rooms = rooms.read().await;
            let peer2room = peer2room.read().await;
            handle_binary_data(peer_id, &tx, data.to_vec(), &rooms, &peer2room).await?;
        }
        ClientMessage::CreateRoom => {
            handle_create_room(peer_id, tx.clone(), &mut rooms_lock, &mut peer2room_lock).await?
        }
        ClientMessage::JoinRoom(room_id) => {
            handle_join_room(peer_id, room_id, &tx, &mut rooms_lock, &mut peer2room_lock).await?
        }
        ClientMessage::LeaveRoom => {
            handle_leave_room(peer_id, &tx, &mut rooms_lock, &mut peer2room_lock).await?
        }
        ClientMessage::Close(_) => {
            handle_leave_room(peer_id, &tx, &mut rooms_lock, &mut peer2room_lock).await?
        }
        ClientMessage::Error(e) => {
            debug!("Processing client error: {}", e);
            send_error(&tx, e).await?
        }
        _ => {}
    }
    Ok(())
}
