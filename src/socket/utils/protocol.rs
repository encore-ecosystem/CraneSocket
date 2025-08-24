use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ConnectionProtocol {
    Tcp,
    WebSocket,
    Udp,
}
