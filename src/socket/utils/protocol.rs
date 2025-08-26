use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionProtocol {
    Tcp,
    WebSocket,
    Udp,
}
