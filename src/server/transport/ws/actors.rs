use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::server::constant::{MAX_ERRORS_ALLOWED, MAX_MESSAGE_SIZE};
use crate::server::error::ProxyServerError;
use crate::server::message::ServerTextMessage;
use crate::server::transport::TransportMessage;
use crate::server::transport::ws::utils::send_max_errors_reached_msg;

async fn app2socket_process_message(
    peer_id: &str,
    msg: TransportMessage,
    ws_sender: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
) -> Result<(), ProxyServerError> {
    let result = match msg {
        TransportMessage::Text(text) => {
            debug!("Sending text message to peer_id={}: {}", peer_id, text);
            ws_sender.send(Message::text(text)).await
        }
        TransportMessage::Binary(data) => {
            debug!(
                "Sending binary message to peer_id={}, size={}",
                peer_id,
                data.len()
            );
            ws_sender.send(Message::binary(data)).await
        }
    };

    match result {
        Ok(()) => Ok(()),
        Err(_) => Err(ProxyServerError::Send("Could not send message".into())),
    }
}

pub async fn app2socket_actor(
    peer_id: String,
    mut ws_sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    mut send_rx: mpsc::Receiver<TransportMessage>,
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
    message: Message,
    recv_tx: &mpsc::Sender<TransportMessage>,
) -> Result<(), ProxyServerError> {
    let message_len = message.len();
    if message_len > MAX_MESSAGE_SIZE {
        let error_msg =
            serde_json::to_string(&ServerTextMessage::Error("Message too large".into()))
                .unwrap_or_default();
        if let Err(e) = recv_tx.send(TransportMessage::Text(error_msg)).await {
            error!(
                "Failed to notify client about oversized message, peer_id={}. Error: {}",
                peer_id, e
            );
        }
        return Ok(());
    }

    let result = match message {
        Message::Text(text) => {
            debug!("Received text message from peer_id={}: {}", peer_id, text);
            recv_tx.send(TransportMessage::Text(text.to_string())).await
        }
        Message::Binary(data) => {
            debug!(
                "Received binary message from peer_id={}, size={}",
                peer_id,
                data.len()
            );
            recv_tx.send(TransportMessage::Binary(data.to_vec())).await
        }
        Message::Close(frame) => {
            info!(
                "Received close message from peer_id={}: {:?}",
                peer_id, frame
            );
            let msg = serde_json::to_string(&ServerTextMessage::ClientLeft).unwrap_or_default();
            recv_tx.send(TransportMessage::Text(msg)).await
        }
        Message::Ping(_) | Message::Pong(_) => {
            debug!(
                "Received control message from peer_id={}: {:?}",
                peer_id, message
            );
            Ok(())
        }
        _ => {
            error!("Unsupported message type from peer_id={}", peer_id,);
            Err(ProxyServerError::InvalidData(
                "Unsupported message type".into(),
            ))?
        }
    };

    result.map_err(|_| ProxyServerError::Send("Could not send message to app".into()))
}

pub async fn socket2app_actor(
    peer_id: String,
    mut ws_receiver: SplitStream<WebSocketStream<TcpStream>>,
    recv_tx: mpsc::Sender<TransportMessage>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut error_counter: usize = 0;
    loop {
        tokio::select! {
            biased;
            Some(msg) = ws_receiver.next() => {
                match msg {
                    Ok(msg) => {
                        match socket2app_process_message(&peer_id, msg, &recv_tx).await {
                            Ok(()) => error_counter = 0,
                            Err(_) => {
                                error_counter += 1;
                                error!(
                                    "WebSocket receive error, peer_id={}, error_count={}",
                                    peer_id, error_counter
                                );

                                if error_counter >= MAX_ERRORS_ALLOWED {
                                    let msg = serde_json::to_string(&ServerTextMessage::ClientLeft)
                                        .unwrap_or_default();
                                    let _ = recv_tx.send(TransportMessage::Text(msg)).await;
                                    send_max_errors_reached_msg(&peer_id, &shutdown_tx);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error_counter += 1;
                        error!(
                            "WebSocket receive error, peer_id={}, error_count={}. Error: {}",
                            peer_id, error_counter, e
                        );

                        if error_counter >= MAX_ERRORS_ALLOWED {
                            let msg = serde_json::to_string(&ServerTextMessage::ClientLeft)
                                .unwrap_or_default();
                            let _ = recv_tx.send(TransportMessage::Text(msg)).await;
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
