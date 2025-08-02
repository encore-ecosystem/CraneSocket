use futures::{SinkExt, StreamExt};
use log::error;
use tokio::net::TcpStream;

use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

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
