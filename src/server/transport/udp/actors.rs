use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use log::{debug, error};
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::server::constant::{MAX_ERRORS_ALLOWED, MAX_MESSAGE_SIZE};
use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::socket::ListenerError;
use crate::socket::proxy::UdpListener;

async fn app2socket_process_message(
    addr: &SocketAddr,
    msg: ServerMessage,
    socket: &UdpListener,
) -> Result<(), ProxyServerError> {
    match socket.send_to(&msg.as_bytes(), addr).await {
        Ok(bytes_sent) => {
            debug!(
                "Seccessfully sent message ({} bytes) to {}",
                bytes_sent, addr
            );
            Ok(())
        }
        Err(_) => {
            error!("Failed to send message to {}", addr,);
            Err(ProxyServerError::Send("Could not send message".into()))
        }
    }
}

pub async fn app2socket_actor(
    socket: Arc<UdpListener>,
    mut send_rx: mpsc::Receiver<(ServerMessage, SocketAddr)>,
    mut shutdown_rx: broadcast::Receiver<()>,
    error_counter_per_client: Arc<RwLock<HashMap<SocketAddr, usize>>>,
) {
    loop {
        tokio::select! {
            biased;
            Some((msg, addr)) = send_rx.recv() => {
                if let Err(e) = app2socket_process_message(&addr, msg, &socket).await
                {
                    let mut error_counter_per_client_lock = error_counter_per_client.write().await;
                    let error_count = error_counter_per_client_lock.entry(addr).or_insert(0);
                    *error_count += 1;
                    error!(
                        "WebSocket send error, peer_id={}, error_count={}: {}",
                        addr, error_count, e
                    );
                }
            },
            _ = shutdown_rx.recv() => {
                while let Ok((msg, addr)) = send_rx.try_recv() {
                    let _ = app2socket_process_message(&addr, msg, &socket).await;
                }
                break;
            }
        }
    }
    debug!("App2Socket actor terminated");
}

async fn socket2app_process_message(
    addr: SocketAddr,
    msg: Vec<u8>,
    recv_tx: &mpsc::Sender<(ClientMessage, SocketAddr)>,
) -> Result<(), ProxyServerError> {
    let message_len = msg.len();
    if message_len > MAX_MESSAGE_SIZE {
        if let Err(e) = recv_tx
            .send((ClientMessage::Error("Message too large".into()), addr))
            .await
        {
            error!(
                "Failed to notify client about oversized message, peer_id={}. Error: {}",
                addr, e
            );
        }
        return Ok(());
    }

    let msg = match ClientMessage::try_from(&msg[..]) {
        Ok(msg) => msg,
        Err(e) => {
            if let Err(e) = recv_tx
                .send((ClientMessage::Error("Invalid message".into()), addr))
                .await
            {
                error!(
                    "Failed to notify client about invalid message, peer_id={}. Error: {}",
                    addr, e
                );
            }

            error!("Received invalid message, peer_id={}. Error: {}", addr, e);
            return Err(ProxyServerError::InvalidData(
                "Received invalid message".into(),
            ));
        }
    };
    let result = recv_tx.send((msg, addr)).await;

    result.map_err(|_| ProxyServerError::Send("Could not send message to app".into()))
}

pub async fn socket2app_actor(
    socket: Arc<UdpListener>,
    recv_tx: mpsc::Sender<(ClientMessage, SocketAddr)>,
    mut shutdown_rx: broadcast::Receiver<()>,
    error_counter_per_client: Arc<RwLock<HashMap<SocketAddr, usize>>>,
) {
    loop {
        tokio::select! {
            biased;
            res = socket.next_from() => {
                match res {
                    Ok((msg, addr)) => {
                        debug!("Received message from peer_id={}", addr);
                        match socket2app_process_message(addr, msg, &recv_tx).await {
                            Ok(()) => {
                                let mut error_counter_per_client_lock = error_counter_per_client.write().await;
                                error_counter_per_client_lock.insert(addr, 0);}
                            Err(e) => {
                                let mut error_counter_per_client_lock = error_counter_per_client.write().await;
                                let error_count = error_counter_per_client_lock.entry(addr).or_insert(0);
                                *error_count += 1;
                                error!(
                                    "Receive invalid datagram from {}, error_count={}. Error: {}",
                                    addr, error_count, e
                                );

                                if *error_count >= MAX_ERRORS_ALLOWED {
                                    let _ = recv_tx.send((ClientMessage::Error("Invalid message".into()), addr)).await;
                                    let _ = recv_tx.send((ClientMessage::LeaveRoom, addr)).await;
                                    error_counter_per_client_lock.remove(&addr);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let ListenerError::InvalidDatagramFrom(addr) = e {
                            let mut error_counter_per_client_lock = error_counter_per_client.write().await;
                            let error_count = error_counter_per_client_lock.entry(addr).or_insert(0);
                            *error_count += 1;
                            log::warn!("error_count={}", error_count);
                            error!(
                                "Receive invalid datagram from {}, error_count={}. Error: {}",
                                addr, error_count, e
                            );

                            if *error_count >= MAX_ERRORS_ALLOWED {
                                let _ = recv_tx.send((ClientMessage::Error("Invalid message".into()), addr)).await;
                                let _ = recv_tx.send((ClientMessage::LeaveRoom, addr)).await;
                                error_counter_per_client_lock.remove(&addr);
                        }
                        };
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }
    debug!("Socket2App actor terminated");
}
