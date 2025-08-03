use futures::{SinkExt, StreamExt};
use log::{debug, error};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::server::constant::PEER_CONNECTION_WAIT_TIMEOUT_SEC;
use crate::{
    server::message::{ClientTextMessage, ServerTextMessage},
    socket::ConnectionError,
};

pub async fn join_room(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    room_id: String,
) -> Result<(), ConnectionError> {
    let msg = ClientTextMessage::JoinRoom(room_id);
    server_conn
        .send(Message::text(serde_json::to_string(&msg)?))
        .await?;

    let msg = server_conn.next().await.unwrap()?;
    match serde_json::from_str::<ServerTextMessage>(&msg.into_text()?) {
        Ok(msg) => match msg {
            ServerTextMessage::JoinedSuccessfully => Ok(()),
            e => {
                error!("{:?}", e);
                Err(ConnectionError::UnavailableRoom)
            }
        },

        Err(e) => Err(ConnectionError::SerdeSerialization(e)),
    }
}

pub async fn register(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<String, ConnectionError> {
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
                return Err(ConnectionError::UnexpectedMessage(format!(
                    "Expected RoomCreated message. Got {:?} ",
                    e
                )));
            }
        },

        Err(e) => return Err(ConnectionError::SerdeSerialization(e)),
    };

    Ok(room_id)
}

pub async fn wait_for_another_peer(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), ConnectionError> {
    let timeout_duration = Duration::from_secs(PEER_CONNECTION_WAIT_TIMEOUT_SEC);

    let msg = match timeout(timeout_duration, server_conn.next()).await {
        Ok(Some(Ok(msg))) => msg,
        Ok(Some(Err(e))) => return Err(ConnectionError::WebSocket(e)),
        Ok(None) => return Err(ConnectionError::UnexpectedClose),
        Err(_) => return Err(ConnectionError::Timeout),
    };

    match serde_json::from_str::<ServerTextMessage>(&msg.into_text()?) {
        Ok(msg) => match msg {
            ServerTextMessage::ClientJoined => Ok(()),
            e => Err(ConnectionError::UnexpectedMessage(format!(
                "Expected PeerJoined message. Got {:?} ",
                e
            ))),
        },
        Err(e) => Err(ConnectionError::SerdeSerialization(e)),
    }
}
