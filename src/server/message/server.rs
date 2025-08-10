use std::fmt;

use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::tungstenite::{
    Bytes, Utf8Bytes,
    protocol::{CloseFrame, frame::Frame},
};

use crate::server::message::common::Tags;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMessage {
    #[serde(skip)]
    Text(Utf8Bytes),
    #[serde(skip)]
    Binary(Bytes),
    #[serde(skip)]
    Ping(Bytes),
    #[serde(skip)]
    Pong(Bytes),
    #[serde(skip)]
    Close(Option<CloseFrame>),
    #[serde(skip)]
    Frame(Frame),
    RoomCreated(String),
    JoinedSuccessfully,
    ClientJoined,
    ClientLeft,
    Error(String),
}

impl fmt::Display for ServerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(string) = self.to_text() {
            write!(f, "{string}")
        } else {
            write!(f, "Binary Data<length={}>", self.len())
        }
    }
}

impl From<TungsteniteMessage> for ServerMessage {
    fn from(msg: TungsteniteMessage) -> Self {
        match msg {
            TungsteniteMessage::Text(text) => ServerMessage::Text(text),
            TungsteniteMessage::Binary(data) => {
                if data.is_empty() {
                    return ServerMessage::Binary(Bytes::new());
                }

                let tag = data[0];
                let payload = data.slice(1..);

                match tag {
                    x if x == Tags::RoomCreated as u8 => {
                        let s =
                            String::from_utf8(payload.to_vec()).unwrap_or_else(|_| String::new());
                        ServerMessage::RoomCreated(s)
                    }
                    x if x == Tags::JoinedSuccessfully as u8 => ServerMessage::JoinedSuccessfully,
                    x if x == Tags::ClientJoined as u8 => ServerMessage::ClientJoined,
                    x if x == Tags::ClientLeft as u8 => ServerMessage::ClientLeft,
                    x if x == Tags::ServerError as u8 => {
                        let s =
                            String::from_utf8(payload.to_vec()).unwrap_or_else(|_| String::new());

                        ServerMessage::Error(s)
                    }
                    x if x == Tags::Binary as u8 => ServerMessage::Binary(payload),
                    _ => ServerMessage::Binary(data),
                }
            }
            TungsteniteMessage::Ping(data) => ServerMessage::Ping(data),
            TungsteniteMessage::Pong(data) => ServerMessage::Pong(data),
            TungsteniteMessage::Close(frame) => ServerMessage::Close(frame),
            TungsteniteMessage::Frame(frame) => ServerMessage::Frame(frame),
        }
    }
}

impl From<ServerMessage> for TungsteniteMessage {
    fn from(msg: ServerMessage) -> Self {
        match msg {
            ServerMessage::Text(text) => TungsteniteMessage::Text(text),
            ServerMessage::Ping(data) => TungsteniteMessage::Ping(data),
            ServerMessage::Pong(data) => TungsteniteMessage::Pong(data),
            ServerMessage::Close(frame) => TungsteniteMessage::Close(frame),
            ServerMessage::Frame(frame) => TungsteniteMessage::Frame(frame),
            ServerMessage::Binary(data) => {
                let mut bin = vec![Tags::Binary as u8];
                bin.extend_from_slice(&data);
                TungsteniteMessage::binary(bin)
            }
            ServerMessage::RoomCreated(s) => {
                let mut bin = vec![Tags::RoomCreated as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
            ServerMessage::JoinedSuccessfully => {
                let bin = vec![Tags::JoinedSuccessfully as u8];
                TungsteniteMessage::binary(bin)
            }
            ServerMessage::ClientJoined => {
                let bin = vec![Tags::ClientJoined as u8];
                TungsteniteMessage::binary(bin)
            }
            ServerMessage::ClientLeft => {
                let bin = vec![Tags::ClientLeft as u8];

                TungsteniteMessage::binary(bin)
            }
            ServerMessage::Error(s) => {
                let mut bin = vec![Tags::ServerError as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
        }
    }
}

impl ServerMessage {
    pub fn text<S>(string: S) -> ServerMessage
    where
        S: Into<Utf8Bytes>,
    {
        ServerMessage::Text(string.into())
    }

    pub fn binary<B>(bin: B) -> ServerMessage
    where
        B: Into<Bytes>,
    {
        ServerMessage::Binary(bin.into())
    }
    pub fn is_text(&self) -> bool {
        matches!(*self, ServerMessage::Text(_))
    }

    pub fn is_binary(&self) -> bool {
        matches!(*self, ServerMessage::Binary(_))
    }

    pub fn is_ping(&self) -> bool {
        matches!(*self, ServerMessage::Ping(_))
    }

    pub fn is_pong(&self) -> bool {
        matches!(*self, ServerMessage::Pong(_))
    }

    pub fn is_close(&self) -> bool {
        matches!(*self, ServerMessage::Close(_))
    }

    pub fn len(&self) -> usize {
        match *self {
            ServerMessage::Text(ref string) => string.len(),
            ServerMessage::Binary(ref data)
            | ServerMessage::Ping(ref data)
            | ServerMessage::Pong(ref data) => data.len(),
            ServerMessage::Close(ref data) => data.as_ref().map(|d| d.reason.len()).unwrap_or(0),
            ServerMessage::Frame(ref frame) => frame.len(),
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_data(self) -> Bytes {
        match self {
            ServerMessage::Text(utf8) => utf8.into(),
            ServerMessage::Binary(data) | ServerMessage::Ping(data) | ServerMessage::Pong(data) => {
                data
            }
            ServerMessage::Close(None) => <_>::default(),
            ServerMessage::Close(Some(frame)) => frame.reason.into(),
            ServerMessage::Frame(frame) => frame.into_payload(),
            ServerMessage::RoomCreated(s) => s.into(),
            ServerMessage::Error(s) => s.into(),
            _ => <_>::default(),
        }
    }

    pub fn into_text(self) -> tokio_tungstenite::tungstenite::Result<Utf8Bytes> {
        match self {
            ServerMessage::Text(txt) => Ok(txt),
            ServerMessage::Binary(data) | ServerMessage::Ping(data) | ServerMessage::Pong(data) => {
                Ok(data.try_into()?)
            }
            ServerMessage::Close(None) => Ok(<_>::default()),
            ServerMessage::Close(Some(frame)) => Ok(frame.reason),
            ServerMessage::Frame(frame) => Ok(frame.into_text()?),
            ServerMessage::RoomCreated(s) => Ok(s.into()),
            ServerMessage::Error(s) => Ok(s.into()),
            _ => Ok(<_>::default()),
        }
    }

    pub fn to_text(&self) -> tokio_tungstenite::tungstenite::Result<&str> {
        match *self {
            ServerMessage::Text(ref string) => Ok(string.as_str()),
            ServerMessage::Binary(ref data)
            | ServerMessage::Ping(ref data)
            | ServerMessage::Pong(ref data) => Ok(str::from_utf8(data)?),
            ServerMessage::Close(None) => Ok("Close"),
            ServerMessage::Close(Some(ref frame)) => Ok(&frame.reason),
            ServerMessage::Frame(ref frame) => Ok(frame.to_text()?),
            ServerMessage::RoomCreated(ref room_id) => Ok(room_id),
            ServerMessage::Error(ref e) => Ok(e),
            ServerMessage::JoinedSuccessfully => Ok("Joined Successfully"),
            ServerMessage::ClientLeft => Ok("Client Left"),
            ServerMessage::ClientJoined => Ok("Client Joined"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_encode_decode_roundtrip() {
        let s = "Hello";
        let msg = ServerMessage::Text(s.into());
        let ws_msg: TungsteniteMessage = msg.clone().into();
        let parsed = ServerMessage::from(ws_msg);
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_joined_roundtrip() {
        let msg = ServerMessage::ClientJoined;
        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_message_len() {
        let msg = ServerMessage::Text("123".into());
        assert_eq!(msg.len(), 3);

        let msg = ServerMessage::Binary(vec![1, 2, 3].into());
        assert_eq!(msg.len(), 3);

        let msg = ServerMessage::ClientJoined;
        assert_eq!(msg.len(), 0);
    }

    #[test]
    fn test_server_binary_encode_decode_regular() {
        let msg = ServerMessage::Binary(vec![1, 2, 3].into());
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_server_joined_successfully_roundtrip() {
        let msg = ServerMessage::JoinedSuccessfully;
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_server_room_created_roundtrip() {
        let msg = ServerMessage::RoomCreated("test_room".to_string());
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    // #[test]
    // fn test_server_invalid_tag() {
    //     let bytes = vec![0xFF, 1, 2, 3]; // Unknown tag
    //     let result = ServerMessage::try_from(bytes.as_slice());
    //     assert_eq!(result, Err(MessageError::InvalidTag(0xFF)));
    // }

    // #[test]
    // fn test_server_invalid_utf8() {
    //     let bytes = vec![Tags::RoomCreated as u8, 0xFF, 0xFF, 0xFF]; // Invalid UTF-8
    //     let result = ServerMessage::try_from(bytes.as_slice());
    //     assert_eq!(result, Err(MessageError::InvalidUtf8));
    // }
}
