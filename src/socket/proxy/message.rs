use std::fmt;

use tokio_tungstenite::tungstenite::{
    Bytes,
    protocol::{CloseFrame, frame::Frame},
};
use tokio_tungstenite::tungstenite::{Message as TungsteniteMessage, Utf8Bytes};

#[derive(Debug)]
pub enum Message {
    Text(Utf8Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
    Frame(Frame),
    PeerDisconnected,
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(string) = self.to_text() {
            write!(f, "{string}")
        } else {
            write!(f, "Binary Data<length={}>", self.len())
        }
    }
}

impl From<TungsteniteMessage> for Message {
    fn from(msg: TungsteniteMessage) -> Self {
        match msg {
            TungsteniteMessage::Text(text) => Message::Text(text),
            TungsteniteMessage::Binary(data) => Message::Binary(data),
            TungsteniteMessage::Ping(data) => Message::Ping(data),
            TungsteniteMessage::Pong(data) => Message::Pong(data),
            TungsteniteMessage::Close(frame) => Message::Close(frame),
            TungsteniteMessage::Frame(frame) => Message::Frame(frame),
        }
    }
}

impl TryFrom<Message> for TungsteniteMessage {
    type Error = &'static str;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        match msg {
            Message::Text(text) => Ok(TungsteniteMessage::Text(text)),
            Message::Binary(data) => Ok(TungsteniteMessage::Binary(data)),
            Message::Ping(data) => Ok(TungsteniteMessage::Ping(data)),
            Message::Pong(data) => Ok(TungsteniteMessage::Pong(data)),
            Message::Close(frame) => Ok(TungsteniteMessage::Close(frame)),
            Message::Frame(frame) => Ok(TungsteniteMessage::Frame(frame)),
            Message::PeerDisconnected => {
                Err("PeerLeft cannot be converted to tungstenite::Message")
            }
        }
    }
}

impl Message {
    pub fn text<S>(string: S) -> Message
    where
        S: Into<Utf8Bytes>,
    {
        Message::Text(string.into())
    }

    pub fn binary<B>(bin: B) -> Message
    where
        B: Into<Bytes>,
    {
        Message::Binary(bin.into())
    }
    pub fn is_text(&self) -> bool {
        matches!(*self, Message::Text(_))
    }

    pub fn is_binary(&self) -> bool {
        matches!(*self, Message::Binary(_))
    }

    pub fn is_ping(&self) -> bool {
        matches!(*self, Message::Ping(_))
    }

    pub fn is_pong(&self) -> bool {
        matches!(*self, Message::Pong(_))
    }

    pub fn is_close(&self) -> bool {
        matches!(*self, Message::Close(_))
    }

    pub fn len(&self) -> usize {
        match *self {
            Message::Text(ref string) => string.len(),
            Message::Binary(ref data) | Message::Ping(ref data) | Message::Pong(ref data) => {
                data.len()
            }
            Message::Close(ref data) => data.as_ref().map(|d| d.reason.len()).unwrap_or(0),
            Message::Frame(ref frame) => frame.len(),
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_data(self) -> Bytes {
        match self {
            Message::Text(utf8) => utf8.into(),
            Message::Binary(data) | Message::Ping(data) | Message::Pong(data) => data,
            Message::Close(None) => <_>::default(),
            Message::Close(Some(frame)) => frame.reason.into(),
            Message::Frame(frame) => frame.into_payload(),
            Message::PeerDisconnected => <_>::default(),
        }
    }

    pub fn into_text(self) -> tokio_tungstenite::tungstenite::Result<Utf8Bytes> {
        match self {
            Message::Text(txt) => Ok(txt),
            Message::Binary(data) | Message::Ping(data) | Message::Pong(data) => {
                Ok(data.try_into()?)
            }
            Message::Close(None) => Ok(<_>::default()),
            Message::Close(Some(frame)) => Ok(frame.reason),
            Message::Frame(frame) => Ok(frame.into_text()?),
            Message::PeerDisconnected => Ok(<_>::default()),
        }
    }

    pub fn to_text(&self) -> tokio_tungstenite::tungstenite::Result<&str> {
        match *self {
            Message::Text(ref string) => Ok(string.as_str()),
            Message::Binary(ref data) | Message::Ping(ref data) | Message::Pong(ref data) => {
                Ok(str::from_utf8(data)?)
            }
            Message::Close(None) => Ok("Close"),
            Message::Close(Some(ref frame)) => Ok(&frame.reason),
            Message::Frame(ref frame) => Ok(frame.to_text()?),
            Message::PeerDisconnected => Ok("Peer Disconnected"),
        }
    }
}
