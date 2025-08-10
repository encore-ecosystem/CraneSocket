use log::{debug, error};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::server::constant::{MAX_ERRORS_ALLOWED, MAX_MESSAGE_SIZE};
use crate::server::error::ProxyServerError;
use crate::server::message::ClientMessage;
use crate::server::message::ServerMessage;
use crate::server::transport::tcp::utils::send_max_errors_reached_msg;
use crate::socket::proxy::ReadHalf;
use crate::socket::proxy::WriteHalf;

async fn app2socket_process_message(
    peer_id: &str,
    msg: ServerMessage,
    ws_sender: &mut WriteHalf,
) -> Result<(), ProxyServerError> {
    match ws_sender.send(&msg.as_bytes()).await {
        Ok(()) => {
            debug!("Seccessfully sent message to peer_id={}", peer_id,);
            Ok(())
        }
        Err(_) => {
            debug!("Failed to send message to peer_id={}", peer_id,);
            Err(ProxyServerError::Send("Could not send message".into()))
        }
    }
}

pub async fn app2socket_actor(
    peer_id: String,
    mut ws_sender: WriteHalf,
    mut send_rx: mpsc::Receiver<ServerMessage>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut error_counter: usize = 0;
    loop {
        tokio::select! {
            biased;
            Some(msg) = send_rx.recv() => {
                if let Err(e) = app2socket_process_message(&peer_id, msg, &mut ws_sender).await
                {
                    error_counter += 1;
                    error!(
                        "WebSocket send error, peer_id={}, error_count={}: {}",
                        peer_id, error_counter, e
                    );

                    if error_counter >= MAX_ERRORS_ALLOWED {
                        match shutdown_tx.send(()) {
                            Ok(_) => {
                                debug!(
                                    "Successfully notified Socket2App actor about MaxErrorsReached"
                                )
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to notify Socket2App actor about MaxErrorsReached: {}",
                                    e
                                )
                            }
                        }

                        break;
                    }
                } else {
                    error_counter = 0;
                }
            },
            _ = shutdown_rx.recv() => {
                while let Ok(msg) = send_rx.try_recv() {
                    let _ = app2socket_process_message(&peer_id, msg, &mut ws_sender).await;
                }
                break;
            }
        }
    }
    debug!("App2Socket actor terminated, peer_id={}", peer_id);
}

async fn socket2app_process_message(
    peer_id: &str,
    msg: Vec<u8>,
    recv_tx: &mpsc::Sender<ClientMessage>,
) -> Result<(), ProxyServerError> {
    let message_len = msg.len();
    if message_len > MAX_MESSAGE_SIZE {
        if let Err(e) = recv_tx
            .send(ClientMessage::Error("Message too large".into()))
            .await
        {
            error!(
                "Failed to notify client about oversized message, peer_id={}. Error: {}",
                peer_id, e
            );
        }
        return Ok(());
    }

    let msg = match ClientMessage::try_from(&msg[..]) {
        Ok(msg) => msg,
        Err(e) => {
            if let Err(e) = recv_tx
                .send(ClientMessage::Error("Invalid message".into()))
                .await
            {
                error!(
                    "Failed to notify client about invalid message, peer_id={}. Error: {}",
                    peer_id, e
                );
            }

            error!(
                "Received invalid message, peer_id={}. Error: {}",
                peer_id, e
            );
            return Ok(());
        }
    };
    let result = recv_tx.send(msg).await;

    result.map_err(|_| ProxyServerError::Send("Could not send message to app".into()))
}

pub async fn socket2app_actor(
    peer_id: String,
    mut ws_receiver: ReadHalf,
    recv_tx: mpsc::Sender<ClientMessage>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut error_counter: usize = 0;
    loop {
        tokio::select! {
            biased;
            data = ws_receiver.next() => {
                match data {
                    Ok(msg) => {
                        debug!("Received message from peer_id={}", peer_id);
                        match socket2app_process_message(&peer_id, msg, &recv_tx).await {
                            Ok(()) => error_counter = 0,
                            Err(_) => {
                                error_counter += 1;
                                error!(
                                    "Receive message error, peer_id={}, error_count={}",
                                    peer_id, error_counter
                                );

                                if error_counter >= MAX_ERRORS_ALLOWED {
                                    let _ = recv_tx.send(ClientMessage::LeaveRoom).await;
                                    send_max_errors_reached_msg(&peer_id, &shutdown_tx);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error_counter += 1;
                        error!(
                            "Receive message error, peer_id={}, error_count={}. Error: {}",
                            peer_id, error_counter, e
                        );

                        if error_counter >= MAX_ERRORS_ALLOWED {
                            let _ = recv_tx.send(ClientMessage::LeaveRoom).await;
                            send_max_errors_reached_msg(&peer_id, &shutdown_tx);
                            break;
                        }
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }
    debug!("Socket2App actor terminated, peer_id={}", peer_id);
}
