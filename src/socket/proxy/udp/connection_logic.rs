use log::debug;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::server::constant::{CLIENT_CONNECTION_WAIT_TIMEOUT_SEC, MAX_MESSAGE_SIZE};
use crate::server::message::{ClientMessage, ServerMessage, Tags};
use crate::socket::ConnectionError;

pub async fn join_room(
    server_conn: &mut UdpSocket,
    room_id: String,
) -> Result<(), ConnectionError> {
    let msg = ClientMessage::JoinRoom(room_id);
    server_conn.send(&msg.as_bytes()).await.unwrap();
    debug!("Sent join room request");

    let mut tag = [0u8; 1];
    let len = server_conn.recv(&mut tag).await?;
    if len != 1 {
        return Err(ConnectionError::UnexpectedMessage(format!(
            "Expected message with len=1. Got len={}",
            len
        )));
    }
    debug!("Received tag sent by server");
    match tag[0] {
        x if x == Tags::JoinedSuccessfully as u8 => Ok(()),
        _ => Err(ConnectionError::RoomUnavailable),
    }
}

pub async fn register(server_conn: &mut UdpSocket) -> Result<String, ConnectionError> {
    let msg = ClientMessage::CreateRoom;
    server_conn.send(&msg.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
    let len = server_conn.recv(&mut buf).await?;
    let msg = ServerMessage::try_from(&buf[..len])?;
    match msg {
        ServerMessage::RoomCreated(room_id) => Ok(room_id),
        msg => Err(ConnectionError::UnexpectedMessage(format!(
            "Expected RoomCreated. Got {}",
            msg
        ))),
    }
}

pub async fn wait_for_another_peer(server_conn: &mut UdpSocket) -> Result<(), ConnectionError> {
    let timeout_duration = Duration::from_secs(CLIENT_CONNECTION_WAIT_TIMEOUT_SEC);

    let result = timeout(timeout_duration, async {
        let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
        let len = server_conn.recv(&mut buf).await?;
        if len != 1 {
            return Err(ConnectionError::UnexpectedMessage(format!(
                "Expected ClientJoined with len=1. Got msg with len={}",
                len
            )));
        }
        Ok(buf[0])
    })
    .await;

    let tag = match result {
        Ok(Ok(tag)) => tag,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(ConnectionError::Timeout),
    };

    match tag {
        x if x == Tags::ClientJoined as u8 => Ok(()),
        _ => Err(ConnectionError::UnexpectedMessage(format!(
            "Expected ClientJoined message (tag {}), got tag {}",
            Tags::ClientJoined as u8,
            tag
        ))),
    }
}

pub async fn leave_room(server_conn: &mut UdpSocket) -> Result<(), ConnectionError> {
    let msg = ClientMessage::LeaveRoom;
    match server_conn.send(&msg.as_bytes()).await {
        Ok(bytes_sent) => {
            debug!("Sent leave room message. {} bytes", { bytes_sent });
            Ok(())
        }
        Err(e) => Err(ConnectionError::Io(e)),
    }
}
