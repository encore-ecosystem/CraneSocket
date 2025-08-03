use crate::server::message::{
    BinaryMessageTrait, ClientBinaryMessage, ClientTextMessage, ServerBinaryMessage,
    ServerTextMessage,
};
use crate::socket::ConnectionError;
use crate::socket::proxy::Message;

use futures::StreamExt;
use futures_util::SinkExt;

use log::debug;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

pub async fn send<
    S: SinkExt<TungsteniteMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
>(
    stream: &mut S,
    msg: Message,
) -> Result<(), ConnectionError> {
    match msg {
        Message::Text(text) => {
            let msg = ClientTextMessage::Data(text.to_string());
            let msg_json = &serde_json::to_string(&msg)?;
            Ok(stream.send(TungsteniteMessage::text(msg_json)).await?)
        }
        Message::Binary(bytes) => {
            let msg = ClientBinaryMessage::Data(bytes.to_vec());
            Ok(stream
                .send(TungsteniteMessage::binary(msg.encode()))
                .await?)
        }
        Message::Close(frame) => Ok(stream.send(TungsteniteMessage::Close(frame)).await?),
        msg => Err(ConnectionError::UnexpectedMessage(format!("{:?}", msg))),
    }
}

pub async fn next<
    S: StreamExt<Item = Result<TungsteniteMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
>(
    stream: &mut S,
) -> Result<Message, ConnectionError> {
    match stream.next().await.unwrap() {
        Ok(msg) => match msg {
            TungsteniteMessage::Text(text) => {
                match serde_json::from_str::<ServerTextMessage>(&text) {
                    Ok(msg) => match msg {
                        ServerTextMessage::Data(text) => Ok(Message::text(text)),
                        ServerTextMessage::ClientLeft => Ok(Message::PeerDisconnected),
                        msg => {
                            debug!("Received unexpected message: {:?}", msg);
                            Err(ConnectionError::UnexpectedMessage(format!("{:?}", msg)))
                        }
                    },

                    Err(e) => Err(ConnectionError::SerdeSerialization(e)),
                }
            }
            TungsteniteMessage::Binary(bytes) => match ServerBinaryMessage::decode(&bytes) {
                Ok(msg) => match msg {
                    ServerBinaryMessage::Data(data) => Ok(Message::binary(data)),
                },
                Err(_) => Err(ConnectionError::Serialization),
            },
            TungsteniteMessage::Close(frame) => Ok(Message::Close(frame)),

            msg => {
                debug!("Received unexpected message: {}", msg);
                Err(ConnectionError::UnexpectedMessage(format!("{:?}", msg)))
            }
        },
        Err(e) => Err(ConnectionError::WebSocket(e)),
    }
}
