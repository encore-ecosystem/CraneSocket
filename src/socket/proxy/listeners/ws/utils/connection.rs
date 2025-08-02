use std::time::Duration;

use futures::{SinkExt, StreamExt};
use log::debug;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::server::constant::PEER_CONNECTION_WAIT_TIMEOUT_SEC;
use crate::{
    server::message::{ClientTextMessage, ServerTextMessage},
    socket::ListenerError,
};

pub async fn register(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<String, ListenerError> {
    let msg = ClientTextMessage::CreateRoom;
    server_conn
        .send(Message::text(serde_json::to_string(&msg)?))
        .await?;
    debug!("Connected to proxy server");

    let msg = server_conn.next().await.unwrap()?;
    debug!("Received server message");
    let room_id = match serde_json::from_str::<ServerTextMessage>(&msg.into_text()?) {
        Ok(msg) => match msg {
            ServerTextMessage::RoomCreated { room_id } => room_id,
            e => {
                return Err(ListenerError::UnexpectedMessage(format!(
                    "Expected RoomCreated message. Got {:?} ",
                    e
                )));
            }
        },

        Err(e) => return Err(ListenerError::SerdeSerialization(e)),
    };

    Ok(room_id)
}

pub async fn wait_for_another_peer(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), ListenerError> {
    let timeout_duration = Duration::from_secs(PEER_CONNECTION_WAIT_TIMEOUT_SEC);

    let msg = match timeout(timeout_duration, server_conn.next()).await {
        Ok(Some(Ok(msg))) => msg,
        Ok(Some(Err(e))) => return Err(ListenerError::WebSocket(e)),
        Ok(None) => return Err(ListenerError::UnexpectedClose),
        Err(_) => return Err(ListenerError::Timeout),
    };

    match serde_json::from_str::<ServerTextMessage>(&msg.into_text()?) {
        Ok(msg) => match msg {
            ServerTextMessage::ClientJoined => Ok(()),
            e => Err(ListenerError::UnexpectedMessage(format!(
                "Expected PeerJoined message. Got {:?} ",
                e
            ))),
        },
        Err(e) => Err(ListenerError::SerdeSerialization(e)),
    }
}
