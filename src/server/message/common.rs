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
    ServerClosed = 5,
    // ClientMessage variants
    CreateRoom = 6,
    JoinRoom = 7,
    LeaveRoom = 8,
    // Shared variants (used by both)
    Error = 9,
    Text = 10,
    Binary = 11,
    Ping = 12,
    Pong = 13,
    Close = 14,
    Frame = 15,
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
