use std::net::SocketAddr;

use log::debug;
use log::error;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::server::Client2Room;
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
    client2room: Client2Room,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(), ProxyServerError> {
    let addr = transport.get_client_addr();
    let tx = transport.get_sender();
    let mut shutdown_rx = shutdown_tx.subscribe();

    loop {
        tokio::select! {
            biased;
            Some(msg_result) = transport.recv() => {
                match msg_result {
                    Ok((msg, addr)) => {
                        if let Err(e) = process_message(addr, msg, tx.clone(), &rooms, &client2room).await {
                            error!("Error processing message for peer {}: {:?}", addr, e);
                            send_error(&addr, &tx, "Internal server error".to_string()).await?;
                        }
                    },
                    Err(e) => {
                        match addr {
                            Some(addr) => error!("Transport error for {}: {:?}", addr, e),
                            None => error!("UDP Transport error: {:?}", e),
                        };
                        let _ = shutdown_tx.send(());
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                match addr {
                    Some(addr) => debug!("Shutdown signal received for peer: {}", addr),
                    None => error!("Shutdown signal received"),
                };
                while let Ok((msg, addr)) = transport.try_recv() {
                    if let Err(e) = process_message(addr, msg, tx.clone(), &rooms, &client2room).await {
                        error!("Error flushing message for peer {}: {:?}", addr, e);
                    }
                }
                let mut rooms_lock = rooms.write().await;
                let mut client2room_lock = client2room.write().await;
                match addr {
                    Some(addr) => handle_leave_room(addr, &tx, &mut rooms_lock, &mut client2room_lock).await?,
                    None => close_server(&tx, &mut client2room_lock).await?,
                };
                break;
            }
        }
    }

    Ok(())
}

async fn process_message(
    addr: SocketAddr,
    msg: ClientMessage,
    tx: mpsc::Sender<(ServerMessage, SocketAddr)>,
    rooms: &Rooms,
    client2room: &Client2Room,
) -> Result<(), ProxyServerError> {
    let mut rooms_lock = rooms.write().await;
    let mut client2room_lock = client2room.write().await;
    match msg {
        ClientMessage::Text(data) => {
            drop((rooms_lock, client2room_lock));
            let rooms = rooms.read().await;
            let client2room = client2room.read().await;
            handle_text_data(addr, &tx, data.to_string(), &rooms, &client2room).await?
        }
        ClientMessage::Binary(data) => {
            drop((rooms_lock, client2room_lock));
            let rooms = rooms.read().await;
            let client2room = client2room.read().await;
            handle_binary_data(addr, &tx, data.to_vec(), &rooms, &client2room).await?;
        }
        ClientMessage::CreateRoom => {
            handle_create_room(addr, tx.clone(), &mut rooms_lock, &mut client2room_lock).await?
        }
        ClientMessage::JoinRoom(room_id) => {
            handle_join_room(addr, room_id, &tx, &mut rooms_lock, &mut client2room_lock).await?
        }
        ClientMessage::LeaveRoom => {
            handle_leave_room(addr, &tx, &mut rooms_lock, &mut client2room_lock).await?
        }
        ClientMessage::Close(_) => {
            handle_leave_room(addr, &tx, &mut rooms_lock, &mut client2room_lock).await?
        }
        ClientMessage::Ping(frame) => handle_ping_msg(&addr, &tx, frame).await?,
        ClientMessage::Error(e) => {
            debug!("Processing client error: {}", e);
            send_error(&addr, &tx, e).await?
        }
        msg => {
            let error_msg = format!("Received unexpected message: {:?}", msg);
            debug!("{}", error_msg);
            send_error(&addr, &tx, error_msg).await?
        }
    }
    Ok(())
}
