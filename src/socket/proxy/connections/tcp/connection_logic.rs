use log::debug;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::server::constant::CLIENT_CONNECTION_WAIT_TIMEOUT_SEC;
use crate::server::message::{ClientMessage, Tags};
use crate::socket::ConnectionError;

pub async fn join_room(
    server_conn: &mut TcpStream,
    room_id: String,
) -> Result<(), ConnectionError> {
    let msg = ClientMessage::JoinRoom(room_id);
    server_conn.write_all(&msg.as_bytes()).await.unwrap();
    debug!("Sent join room request");

    let mut tag = [0u8; 1];
    server_conn.read_exact(&mut tag).await?;
    debug!("Read tag sent by server");
    match tag[0] {
        x if x == Tags::JoinedSuccessfully as u8 => Ok(()),
        _ => Err(ConnectionError::RoomUnavailable),
    }
}

pub async fn register(server_conn: &mut TcpStream) -> Result<String, ConnectionError> {
    let msg = ClientMessage::CreateRoom;
    server_conn.write_all(&msg.as_bytes()).await.unwrap();

    let mut tag = [0u8; 1];
    server_conn.read_exact(&mut tag).await?;
    match tag[0] {
        x if x == Tags::RoomCreated as u8 => {
            let mut buf = [0u8; 8];
            server_conn.read_exact(&mut buf).await?;
            let len = usize::from_be_bytes(buf);

            let mut payload = vec![0u8; len];
            server_conn.read_exact(&mut payload).await?;
            let room_id = String::from_utf8(payload).unwrap();
            Ok(room_id)
        }
        _ => Err(ConnectionError::RoomUnavailable),
    }
}

pub async fn wait_for_another_client(server_conn: &mut TcpStream) -> Result<(), ConnectionError> {
    let timeout_duration = Duration::from_secs(CLIENT_CONNECTION_WAIT_TIMEOUT_SEC);

    let result = timeout(timeout_duration, async {
        let mut tag = [0u8; 1];
        server_conn.read_exact(&mut tag).await?;
        Ok(tag[0])
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
