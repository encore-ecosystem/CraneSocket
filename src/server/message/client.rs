use std::fmt;

use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::tungstenite::protocol::frame::FrameHeader;
use tokio_tungstenite::tungstenite::{
    Bytes, Utf8Bytes,
    protocol::{CloseFrame, frame::Frame},
};

use crate::server::message::common::{MessageError, Tags};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Text(Utf8Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
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
                    x if x == Tags::Error as u8 => {
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
                let mut bin = vec![Tags::Error as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
        }
    }
}
impl From<ClientMessage> for Vec<u8> {
    fn from(msg: ClientMessage) -> Self {
        match msg {
            ClientMessage::Text(text) => {
                let mut bin = vec![Tags::Text as u8];
                bin.extend_from_slice(&(text.len() as u64).to_be_bytes());
                bin.extend_from_slice(text.as_bytes());
                bin
            }
            ClientMessage::Binary(data) => {
                let mut bin = vec![Tags::Binary as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ClientMessage::Ping(data) => {
                let mut bin = vec![Tags::Ping as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ClientMessage::Pong(data) => {
                let mut bin = vec![Tags::Pong as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ClientMessage::Close(_) => {
                vec![Tags::Close as u8]
            }
            ClientMessage::Frame(_) => {
                vec![Tags::Frame as u8]
            }
            ClientMessage::CreateRoom => {
                vec![Tags::CreateRoom as u8]
            }
            ClientMessage::JoinRoom(s) => {
                let mut bin = vec![Tags::JoinRoom as u8];
                bin.extend_from_slice(&(s.len() as u64).to_be_bytes());
                bin.extend_from_slice(s.as_bytes());
                bin
            }
            ClientMessage::LeaveRoom => {
                vec![Tags::LeaveRoom as u8]
            }
            ClientMessage::Error(s) => {
                let mut bin = vec![Tags::Error as u8];
                bin.extend_from_slice(&(s.len() as u64).to_be_bytes());
                bin.extend_from_slice(s.as_bytes());
                bin
            }
        }
    }
}

impl TryFrom<&[u8]> for ClientMessage {
    type Error = MessageError;

    fn try_from(data: &[u8]) -> Result<Self, MessageError> {
        if data.is_empty() {
            return Err(MessageError::EmptyData);
        }

        let tag = data[0];

        if data.len() == 1 {
            match tag {
                x if x == Tags::CreateRoom as u8 => Ok(ClientMessage::CreateRoom),
                x if x == Tags::LeaveRoom as u8 => Ok(ClientMessage::LeaveRoom),
                x if x == Tags::Close as u8 => Ok(ClientMessage::Close(None)),
                x if x == Tags::Frame as u8 => Ok(ClientMessage::Frame(Frame::from_payload(
                    FrameHeader::default(),
                    Bytes::new(),
                ))),
                _ => Err(MessageError::InvalidTag(tag)),
            }
        } else {
            if data.len() < 9 {
                return Err(MessageError::InsufficientData);
            }

            let len = u64::from_be_bytes(
                data[1..9]
                    .try_into()
                    .map_err(|_| MessageError::InsufficientData)?,
            ) as usize;

            if data.len() < 9 + len {
                return Err(MessageError::InsufficientData);
            }

            let payload = &data[9..9 + len];

            match tag {
                x if x == Tags::Text as u8 => {
                    let s = String::from_utf8(payload.to_vec())
                        .map_err(|_| MessageError::InvalidUtf8)?;
                    Ok(ClientMessage::text(s))
                }
                x if x == Tags::Binary as u8 => {
                    Ok(ClientMessage::Binary(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::Ping as u8 => {
                    Ok(ClientMessage::Ping(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::Pong as u8 => {
                    Ok(ClientMessage::Pong(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::JoinRoom as u8 => {
                    let s = String::from_utf8(payload.to_vec())
                        .map_err(|_| MessageError::InvalidUtf8)?;
                    Ok(ClientMessage::JoinRoom(s))
                }
                x if x == Tags::Error as u8 => {
                    let s = String::from_utf8(payload.to_vec())
                        .map_err(|_| MessageError::InvalidUtf8)?;
                    Ok(ClientMessage::Error(s))
                }
                _ => Err(MessageError::InvalidTag(tag)),
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

    pub fn as_bytes(&self) -> Vec<u8> {
        self.clone().into()
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
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_create_room_roundtrip() {
        let msg = ClientMessage::CreateRoom;
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_join_room_roundtrip() {
        let msg = ClientMessage::JoinRoom("test_room".to_string());
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ClientMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ClientMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_insufficient_data() {
        let bytes = vec![Tags::JoinRoom as u8, 0xFF, 0xFF, 0xFF];
        let result = ClientMessage::try_from(bytes.as_slice());
        assert_eq!(result, Err(MessageError::InsufficientData));
    }

    #[tokio::test]
    async fn test_client_message_serialization_deserialization() {
        let messages = vec![
            ClientMessage::CreateRoom,
            ClientMessage::LeaveRoom,
            ClientMessage::Close(None),
        ];

        for msg in messages {
            let bytes = msg.as_bytes();
            assert_eq!(bytes.len(), 1, "Expected 1 byte for {:?}", msg);
            let deserialized = ClientMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        let text_messages = vec![
            (
                ClientMessage::text("hello"),
                Tags::Text,
                Bytes::from("hello"),
            ),
            (
                ClientMessage::JoinRoom("room123".to_string()),
                Tags::JoinRoom,
                Bytes::from("room123"),
            ),
            (
                ClientMessage::Error("error msg".to_string()),
                Tags::Error,
                Bytes::from("error msg"),
            ),
        ];

        for (msg, tag, expected_payload) in text_messages {
            let bytes = msg.as_bytes();
            assert_eq!(bytes[0], tag as u8, "Wrong tag for {:?}", msg);
            let len = u64::from_be_bytes(bytes[1..9].try_into().unwrap()) as usize;
            assert_eq!(len, bytes[9..].len(), "Wrong payload length for {:?}", msg);
            assert_eq!(
                Bytes::from(bytes[9..].to_vec()),
                expected_payload,
                "Wrong payload for {:?}",
                msg
            );

            let deserialized = ClientMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        let binary_messages = vec![
            (
                ClientMessage::Binary(Bytes::from_static(&[1, 2, 3])),
                Tags::Binary,
                Bytes::from_static(&[1, 2, 3]),
            ),
            (
                ClientMessage::Ping(Bytes::from_static(&[4, 5])),
                Tags::Ping,
                Bytes::from_static(&[4, 5]),
            ),
            (
                ClientMessage::Pong(Bytes::from_static(&[6, 7])),
                Tags::Pong,
                Bytes::from_static(&[6, 7]),
            ),
        ];

        for (msg, tag, expected_payload) in binary_messages {
            let bytes = msg.as_bytes();
            assert_eq!(bytes[0], tag as u8, "Wrong tag for {:?}", msg);
            let len = u64::from_be_bytes(bytes[1..9].try_into().unwrap()) as usize;
            assert_eq!(len, bytes[9..].len(), "Wrong payload length for {:?}", msg);
            assert_eq!(
                Bytes::from(bytes[9..].to_vec()),
                expected_payload,
                "Wrong payload for {:?}",
                msg
            );

            let deserialized = ClientMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        assert!(matches!(
            ClientMessage::try_from(&[][..]),
            Err(MessageError::EmptyData)
        ));

        assert!(matches!(
            ClientMessage::try_from(&[255][..]),
            Err(MessageError::InvalidTag(255))
        ));

        let invalid_utf8 = vec![Tags::Text as u8]
            .into_iter()
            .chain((5u64).to_be_bytes().into_iter())
            .chain([0xFF, 0xFF, 0xFF, 0xFF, 0xFF].into_iter())
            .collect::<Vec<u8>>();
        assert!(matches!(
            ClientMessage::try_from(&invalid_utf8[..]),
            Err(MessageError::InvalidUtf8)
        ));

        let insufficient_data = vec![Tags::Text as u8]
            .into_iter()
            .chain((10u64).to_be_bytes().into_iter())
            .chain("short".as_bytes().to_vec().into_iter())
            .collect::<Vec<u8>>();
        assert!(matches!(
            ClientMessage::try_from(&insufficient_data[..]),
            Err(MessageError::InsufficientData)
        ));
    }
}
