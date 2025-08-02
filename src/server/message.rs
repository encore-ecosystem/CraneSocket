use serde::{Deserialize, Serialize};

use crate::server::error::ProxyServerError;

#[derive(Debug, Serialize, Deserialize)]
pub enum CentralMessage {
    CreateRoom,
    JoinRoom(String),
    LeaveRoom,
    Data(&'static [u8]),
    Disconnect(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientTextMessage {
    CreateRoom,
    JoinRoom(String),
    LeaveRoom,
    Data(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerTextMessage {
    RoomCreated { room_id: String },
    ClientJoined,
    JoinedSuccessfully,
    ClientLeft,
    Error(String),
    Data(String),
}

#[derive(Debug)]
pub enum BinaryMessageType {
    Data = 0x00,
}

#[derive(Debug)]
pub enum ServerBinaryMessage {
    Data(Vec<u8>),
}

#[derive(Debug)]
pub enum ClientBinaryMessage {
    Data(Vec<u8>),
}

pub trait BinaryMessageTrait: Send + Sync + 'static {
    fn encode(&self) -> Vec<u8>;
    fn decode(data: &[u8]) -> Result<Self, ProxyServerError>
    where
        Self: Sized;
    fn is_valid(data: &[u8]) -> bool;
    fn get_type(data: &[u8]) -> Option<BinaryMessageType>;
}

impl BinaryMessageTrait for ClientBinaryMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            ClientBinaryMessage::Data(data) => {
                let mut bytes = vec![BinaryMessageType::Data as u8];
                bytes.extend(data);
                bytes
            }
        }
    }

    fn decode(data: &[u8]) -> Result<Self, ProxyServerError> {
        if data.is_empty() {
            return Err(ProxyServerError::InvalidData("Empty binary message".into()));
        }
        let msg_type = data[0];
        let payload = &data[1..];
        match msg_type {
            x if x == BinaryMessageType::Data as u8 => {
                Ok(ClientBinaryMessage::Data(payload.to_vec()))
            }
            _ => Err(ProxyServerError::InvalidData(
                "Unknown binary message type".into(),
            )),
        }
    }

    fn is_valid(data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        matches!(
            data[0],
            x if x == BinaryMessageType::Data as u8
        )
    }

    fn get_type(data: &[u8]) -> Option<BinaryMessageType> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            x if x == BinaryMessageType::Data as u8 => Some(BinaryMessageType::Data),
            _ => None,
        }
    }
}

impl BinaryMessageTrait for ServerBinaryMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            ServerBinaryMessage::Data(data) => {
                let mut bytes = vec![BinaryMessageType::Data as u8];
                bytes.extend(data);
                bytes
            }
        }
    }

    fn decode(data: &[u8]) -> Result<Self, ProxyServerError> {
        if data.is_empty() {
            return Err(ProxyServerError::InvalidData("Empty binary message".into()));
        }
        let msg_type = data[0];
        let payload = &data[1..];
        match msg_type {
            x if x == BinaryMessageType::Data as u8 => {
                Ok(ServerBinaryMessage::Data(payload.to_vec()))
            }
            _ => Err(ProxyServerError::InvalidData(
                "Unknown binary message type".into(),
            )),
        }
    }

    fn is_valid(data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        matches!(
            data[0],
            x if x == BinaryMessageType::Data as u8
        )
    }

    fn get_type(data: &[u8]) -> Option<BinaryMessageType> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            x if x == BinaryMessageType::Data as u8 => Some(BinaryMessageType::Data),
            _ => None,
        }
    }
}
