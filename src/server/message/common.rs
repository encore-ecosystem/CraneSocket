use thiserror::Error;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tags {
    // ServerMessage variants
    RoomCreated = 0x01,
    JoinedSuccessfully = 0x02,
    ClientJoined = 0x03,
    ClientLeft = 0x04,
    ServerError = 0x05,
    // ClientMessage variants
    CreateRoom = 0x06,
    JoinRoom = 0x07,
    LeaveRoom = 0x08,
    ClientError = 0x09,
    // Shared variants (used by both)
    Text = 0x0A,
    Binary = 0x0B,
    Ping = 0x0C,
    Pong = 0x0D,
    Close = 0x0E,
    Frame = 0x0F,
}

#[allow(dead_code)]
#[derive(Error, Debug, PartialEq, Eq)]
pub enum MessageError {
    #[error("InvalidTag: {0}")]
    InvalidTag(u8),
    #[error("InvalidUtf8")]
    InvalidUtf8,
    #[error("EmptyData")]
    EmptyData,
}
