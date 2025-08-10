use std::fmt;

use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::tungstenite::protocol::frame::FrameHeader;
use tokio_tungstenite::tungstenite::{
    Bytes, Utf8Bytes,
    protocol::{CloseFrame, frame::Frame},
};

use crate::server::message::common::{MessageError, Tags};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    Text(Utf8Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
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
                    x if x == Tags::Error as u8 => {
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
                let mut bin = vec![Tags::Error as u8];
                bin.extend_from_slice(s.as_bytes());
                TungsteniteMessage::binary(bin)
            }
        }
    }
}

impl From<ServerMessage> for Vec<u8> {
    fn from(msg: ServerMessage) -> Self {
        match msg {
            ServerMessage::Text(text) => {
                let mut bin = vec![Tags::Text as u8];
                bin.extend_from_slice(&(text.len() as u64).to_be_bytes());
                bin.extend_from_slice(text.as_bytes());
                bin
            }
            ServerMessage::Binary(data) => {
                let mut bin = vec![Tags::Binary as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ServerMessage::Ping(data) => {
                let mut bin = vec![Tags::Ping as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ServerMessage::Pong(data) => {
                let mut bin = vec![Tags::Pong as u8];
                bin.extend_from_slice(&(data.len() as u64).to_be_bytes());
                bin.extend_from_slice(&data);
                bin
            }
            ServerMessage::Close(_) => {
                vec![Tags::Close as u8]
            }
            ServerMessage::Frame(_) => {
                vec![Tags::Frame as u8]
            }
            ServerMessage::RoomCreated(s) => {
                let mut bin = vec![Tags::RoomCreated as u8];
                bin.extend_from_slice(&(s.len() as u64).to_be_bytes());
                bin.extend_from_slice(s.as_bytes());
                bin
            }
            ServerMessage::JoinedSuccessfully => {
                vec![Tags::JoinedSuccessfully as u8]
            }
            ServerMessage::ClientJoined => {
                vec![Tags::ClientJoined as u8]
            }
            ServerMessage::ClientLeft => {
                vec![Tags::ClientLeft as u8]
            }
            ServerMessage::Error(s) => {
                let mut bin = vec![Tags::Error as u8];
                bin.extend_from_slice(&(s.len() as u64).to_be_bytes());
                bin.extend_from_slice(s.as_bytes());
                bin
            }
        }
    }
}

impl TryFrom<&[u8]> for ServerMessage {
    type Error = MessageError;

    fn try_from(data: &[u8]) -> Result<Self, MessageError> {
        if data.is_empty() {
            return Err(MessageError::EmptyData);
        }

        let tag = data[0];

        if data.len() == 1 {
            match tag {
                x if x == Tags::JoinedSuccessfully as u8 => Ok(ServerMessage::JoinedSuccessfully),
                x if x == Tags::ClientJoined as u8 => Ok(ServerMessage::ClientJoined),
                x if x == Tags::ClientLeft as u8 => Ok(ServerMessage::ClientLeft),
                x if x == Tags::Close as u8 => Ok(ServerMessage::Close(None)),
                x if x == Tags::Frame as u8 => Ok(ServerMessage::Frame(Frame::from_payload(
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
                    Ok(ServerMessage::text(s))
                }
                x if x == Tags::Binary as u8 => {
                    Ok(ServerMessage::Binary(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::Ping as u8 => {
                    Ok(ServerMessage::Ping(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::Pong as u8 => {
                    Ok(ServerMessage::Pong(Bytes::copy_from_slice(payload)))
                }
                x if x == Tags::RoomCreated as u8 => {
                    let s = String::from_utf8(payload.to_vec())
                        .map_err(|_| MessageError::InvalidUtf8)?;
                    Ok(ServerMessage::RoomCreated(s))
                }
                x if x == Tags::Error as u8 => {
                    let s = String::from_utf8(payload.to_vec())
                        .map_err(|_| MessageError::InvalidUtf8)?;
                    Ok(ServerMessage::Error(s))
                }
                _ => Err(MessageError::InvalidTag(tag)),
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

    pub fn as_bytes(&self) -> Vec<u8> {
        self.clone().into()
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
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_server_joined_successfully_roundtrip() {
        let msg = ServerMessage::JoinedSuccessfully;
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_server_room_created_roundtrip() {
        let msg = ServerMessage::RoomCreated("test_room".to_string());
        let bytes: Vec<u8> = msg.clone().into();
        let parsed = ServerMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);

        let ws: TungsteniteMessage = msg.clone().into();
        let parsed: ServerMessage = ws.into();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_client_insufficient_data() {
        let bytes = vec![Tags::JoinRoom as u8, 0xFF, 0xFF, 0xFF];
        let result = ServerMessage::try_from(bytes.as_slice());
        assert_eq!(result, Err(MessageError::InsufficientData));
    }

    #[tokio::test]
    async fn test_server_message_serialization_deserialization() {
        let messages = vec![
            ServerMessage::JoinedSuccessfully,
            ServerMessage::ClientJoined,
            ServerMessage::ClientLeft,
            ServerMessage::Close(None),
        ];

        for msg in messages {
            let bytes = msg.as_bytes();
            assert_eq!(bytes.len(), 1, "Expected 1 byte for {:?}", msg);
            let deserialized = ServerMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        let text_messages = vec![
            (
                ServerMessage::text("hello"),
                Tags::Text,
                Bytes::from("hello"),
            ),
            (
                ServerMessage::RoomCreated("room123".to_string()),
                Tags::RoomCreated,
                Bytes::from("room123"),
            ),
            (
                ServerMessage::Error("error msg".to_string()),
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

            let deserialized = ServerMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        let binary_messages = vec![
            (
                ServerMessage::Binary(Bytes::from_static(&[1, 2, 3])),
                Tags::Binary,
                Bytes::from_static(&[1, 2, 3]),
            ),
            (
                ServerMessage::Ping(Bytes::from_static(&[4, 5])),
                Tags::Ping,
                Bytes::from_static(&[4, 5]),
            ),
            (
                ServerMessage::Pong(Bytes::from_static(&[6, 7])),
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

            let deserialized = ServerMessage::try_from(&bytes[..]).unwrap();
            assert_eq!(deserialized, msg, "Deserialization failed for {:?}", msg);
        }

        assert!(matches!(
            ServerMessage::try_from(&[][..]),
            Err(MessageError::EmptyData)
        ));

        assert!(matches!(
            ServerMessage::try_from(&[255][..]),
            Err(MessageError::InvalidTag(255))
        ));

        let invalid_utf8 = vec![Tags::Text as u8]
            .into_iter()
            .chain((5u64).to_be_bytes().into_iter())
            .chain([0xFF, 0xFF, 0xFF, 0xFF, 0xFF].into_iter())
            .collect::<Vec<u8>>();
        assert!(matches!(
            ServerMessage::try_from(&invalid_utf8[..]),
            Err(MessageError::InvalidUtf8)
        ));

        let insufficient_data = vec![Tags::Text as u8]
            .into_iter()
            .chain((10u64).to_be_bytes().into_iter())
            .chain("short".as_bytes().to_vec().into_iter())
            .collect::<Vec<u8>>();
        assert!(matches!(
            ServerMessage::try_from(&insufficient_data[..]),
            Err(MessageError::InsufficientData)
        ));
    }
}
