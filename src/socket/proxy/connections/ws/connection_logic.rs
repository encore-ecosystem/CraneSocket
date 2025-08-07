use futures::{SinkExt, StreamExt};
use log::{debug, error};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::server::constant::PEER_CONNECTION_WAIT_TIMEOUT_SEC;
use crate::server::message::{ClientMessage, ServerMessage};
use crate::socket::ConnectionError;

pub async fn join_room(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    room_id: String,
) -> Result<(), ConnectionError> {
    let msg = ClientMessage::JoinRoom(room_id);
    server_conn.send(msg.into()).await?;

    let msg: ServerMessage = server_conn.next().await.unwrap()?.into();
    match msg {
        ServerMessage::JoinedSuccessfully => Ok(()),
        e => {
            error!("{:?}", e);
            Err(ConnectionError::UnavailableRoom)
        }
    }
}

pub async fn register(
    server_conn: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<String, ConnectionError> {
    let msg = ClientMessage::CreateRoom;
    server_conn.send(msg.into()).await?;
    debug!("Connected to proxy server");

    let msg: ServerMessage = server_conn.next().await.unwrap()?.into();
    debug!("Received server message");
    let room_id = match msg {
        ServerMessage::RoomCreated(room_id) => room_id,
        e => {
            return Err(ConnectionError::UnexpectedMessage(format!(
                "Expected RoomCreated message. Got {:?} ",
                e
            )));
        }
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

    match msg.into() {
        ServerMessage::ClientJoined => Ok(()),
        e => Err(ConnectionError::UnexpectedMessage(format!(
            "Expected PeerJoined message. Got {:?} ",
            e
        ))),
    }
}
