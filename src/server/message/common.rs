use thiserror::Error;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tags {
    // ServerMessage variants
    RoomCreated = 1,
    JoinedSuccessfully = 2,
    ClientJoined = 3,
    ClientLeft = 4,
    // ClientMessage variants
    CreateRoom = 5,
    JoinRoom = 6,
    LeaveRoom = 7,
    // Shared variants (used by both)
    Error = 8,
    Text = 9,
    Binary = 10,
    Ping = 11,
    Pong = 12,
    Close = 13,
    Frame = 14,
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
    #[error("InsufficientData")]
    InsufficientData,
}
