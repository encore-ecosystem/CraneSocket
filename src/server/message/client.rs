use std::fmt;

use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::tungstenite::{
    Bytes, Utf8Bytes,
    protocol::{CloseFrame, frame::Frame},
};

use crate::server::message::common::Tags;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMessage {
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
    CreateRoom,
    JoinRoom(String),
    LeaveRoom,
    Error(String),
}

impl fmt::Display for ClientMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(string) = self.to_text() {
            write!(f, "{string}")
        } else {
            write!(f, "Binary Data<length={}>", self.len())
        }
    }
}

impl From<TungsteniteMessage> for ClientMessage {
    fn from(msg: TungsteniteMessage) -> Self {
        match msg {
            TungsteniteMessage::Text(text) => ClientMessage::Text(text),
            TungsteniteMessage::Binary(data) => {
                if data.is_empty() {
                    return ClientMessage::Binary(Bytes::new());
                }

                let tag = data[0];
                let payload = data.slice(1..);

                match tag {
                    x if x == Tags::JoinRoom as u8 => {
                        let s =
                            String::from_utf8(payload.to_vec()).unwrap_or_else(|_| String::new());
                        ClientMessage::JoinRoom(s)
                    }
                    x if x == Tags::CreateRoom as u8 => ClientMessage::CreateRoom,
                    x if x == Tags::LeaveRoom as u8 => ClientMessage::LeaveRoom,
                    x if x == Tags::ClientError as u8 => {
                        let s =
                            String::from_utf8(payload.to_vec()).unwrap_or_else(|_| String::new());

                        ClientMessage::Error(s)
                    }
                    x if x == Tags::Binary as u8 => ClientMessage::Binary(payload),
                    _ => ClientMessage::Binary(data),
                }
            }
            TungsteniteMessage::Ping(data) => ClientMessage::Ping(data),
            TungsteniteMessage::Pong(data) => ClientMessage::Pong(data),
            TungsteniteMessage::Close(frame) => ClientMessage::Close(frame),
            TungsteniteMessage::Frame(frame) => ClientMessage::Frame(frame),
        }
    }
}

impl From<ClientMessage> for TungsteniteMessage {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::Text(text) => TungsteniteMessage::Text(text),
            ClientMessage::Ping(data) => TungsteniteMessage::Ping(data),
            ClientMessage::Pong(data) => TungsteniteMessage::Pong(data),
            ClientMessage::Close(frame) => TungsteniteMessage::Close(frame),
            ClientMessage::Frame(frame) => TungsteniteMessage::Frame(frame),
            ClientMessage::Binary(data) => {
                let mut bin = vec![Tags::Binary as u8];
                bin.extend_from_slice(&data);
                TungsteniteMessage::binary(bin)
            }
            ClientMessage::CreateRoom => {
                let bin = vec![Tags::CreateRoom as u8];
                TungsteniteMessage::binary(bin)
            }
            ClientMessage::JoinRoom(s) => {
                let mut bin = vec![Tags::JoinRoom as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
            ClientMessage::LeaveRoom => {
                let bin = vec![Tags::LeaveRoom as u8];
                TungsteniteMessage::binary(bin)
            }
            ClientMessage::Error(s) => {
                let mut bin = vec![Tags::ClientError as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
        }
    }
}

impl ClientMessage {
    pub fn text<S>(string: S) -> ClientMessage
    where
        S: Into<Utf8Bytes>,
    {
        ClientMessage::Text(string.into())
    }

    pub fn binary<B>(bin: B) -> ClientMessage
    where
        B: Into<Bytes>,
    {
        ClientMessage::Binary(bin.into())
    }
    pub fn is_text(&self) -> bool {
        matches!(*self, ClientMessage::Text(_))
    }

    pub fn is_binary(&self) -> bool {
        matches!(*self, ClientMessage::Binary(_))
    }

    pub fn is_ping(&self) -> bool {
        matches!(*self, ClientMessage::Ping(_))
    }

    pub fn is_pong(&self) -> bool {
        matches!(*self, ClientMessage::Pong(_))
    }

    pub fn is_close(&self) -> bool {
        matches!(*self, ClientMessage::Close(_))
    }

    pub fn len(&self) -> usize {
        match *self {
            ClientMessage::Text(ref string) => string.len(),
            ClientMessage::Binary(ref data)
            | ClientMessage::Ping(ref data)
            | ClientMessage::Pong(ref data) => data.len(),
            ClientMessage::Close(ref data) => data.as_ref().map(|d| d.reason.len()).unwrap_or(0),
            ClientMessage::Frame(ref frame) => frame.len(),
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_data(self) -> Bytes {
        match self {
            ClientMessage::Text(utf8) => utf8.into(),
            ClientMessage::Binary(data) | ClientMessage::Ping(data) | ClientMessage::Pong(data) => {
                data
            }
            ClientMessage::Close(None) => <_>::default(),
            ClientMessage::Close(Some(frame)) => frame.reason.into(),
            ClientMessage::Frame(frame) => frame.into_payload(),
            ClientMessage::JoinRoom(s) => s.into(),
            ClientMessage::Error(s) => s.into(),
            _ => <_>::default(),
        }
    }

    pub fn into_text(self) -> tokio_tungstenite::tungstenite::Result<Utf8Bytes> {
        match self {
            ClientMessage::Text(txt) => Ok(txt),
            ClientMessage::Binary(data) | ClientMessage::Ping(data) | ClientMessage::Pong(data) => {
                Ok(data.try_into()?)
            }
            ClientMessage::Close(None) => Ok(<_>::default()),
            ClientMessage::Close(Some(frame)) => Ok(frame.reason),
            ClientMessage::Frame(frame) => Ok(frame.into_text()?),
            ClientMessage::JoinRoom(s) => Ok(s.into()),
            ClientMessage::Error(s) => Ok(s.into()),
            _ => Ok(<_>::default()),
        }
    }

    pub fn to_text(&self) -> tokio_tungstenite::tungstenite::Result<&str> {
        match *self {
            ClientMessage::Text(ref string) => Ok(string.as_str()),
            ClientMessage::Binary(ref data)
            | ClientMessage::Ping(ref data)
            | ClientMessage::Pong(ref data) => Ok(str::from_utf8(data)?),
            ClientMessage::Close(None) => Ok("Close"),
            ClientMessage::Close(Some(ref frame)) => Ok(&frame.reason),
            ClientMessage::Frame(ref frame) => Ok(frame.to_text()?),
            ClientMessage::CreateRoom => Ok("Create Room"),
            ClientMessage::JoinRoom(ref room_id) => Ok(room_id),
            ClientMessage::LeaveRoom => Ok("Leave Room"),
            ClientMessage::Error(ref e) => Ok(e),
        }
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;

    #[test]
    fn test_text_encode_decode_roundtrip() {
        let s = "Hello";
        let msg = ClientMessage::Text(s.into());
        let ws_msg: TungsteniteMessage = msg.clone().into();
        let parsed = ClientMessage::from(ws_msg);
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_message_len() {
        let msg = ClientMessage::Text("123".into());
        assert_eq!(msg.len(), 3);

        let msg = ClientMessage::Binary(vec![1, 2, 3].into());
        assert_eq!(msg.len(), 3);

        let msg = ClientMessage::CreateRoom;
        assert_eq!(msg.len(), 0);
    }

    #[test]
    fn test_client_binary_encode_decode_roundtrip() {
        let msg = ClientMessage::Binary(vec![1, 2, 3].into());
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_create_room_roundtrip() {
        let msg = ClientMessage::CreateRoom;
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_join_room_roundtrip() {
        let msg = ClientMessage::JoinRoom("test_room".to_string());
        // let bytes: Vec<u8> = msg.clone().into();
        // let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        // assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    // #[test]
    // fn test_client_invalid_tag() {
    //     let bytes = vec![0xFF, 1, 2, 3]; // Unknown tag
    //     let result = ClientMessage::try_from(bytes.as_slice());
    //     assert_eq!(result, Err(MessageError::InvalidTag(0xFF)));
    // }

    // #[test]
    // fn test_client_invalid_utf8() {
    //     let bytes = vec![Tags::JoinRoom as u8, 0xFF, 0xFF, 0xFF]; // Invalid UTF-8
    //     let result = ClientMessage::try_from(bytes.as_slice());
    //     assert_eq!(result, Err(MessageError::InvalidUtf8));
    // }
}
