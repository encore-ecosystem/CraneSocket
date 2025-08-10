// use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::{
    Bytes, Utf8Bytes,
    protocol::{CloseFrame, frame::Frame},
};

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

fn main() {}
