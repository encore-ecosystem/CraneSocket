#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tags {
    // Shared variants
    Binary = 0x0,
    // ServerMessage variants
    RoomCreated = 0x01,
    JoinedSuccessfully = 0x02,
    ClientJoined = 0x03,
    ClientLeft = 0x04,
    // ClientMessage variants
    CreateRoom = 0x06,
    JoinRoom = 0x07,
    LeaveRoom = 0x08,
    Error = 0x09,
}
