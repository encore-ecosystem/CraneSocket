#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tags {
    Text = 0,
    Binary = 1,
    Ping = 2,
    Pong = 3,
    Close = 4,
    Frame = 5,
}
