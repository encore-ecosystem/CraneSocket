use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ConnectionMethod {
    UPnP,
    HolePunching,
    Stun,
    Proxy,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum BindMethod {
    Local,
    Remote,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ConnectionProtocol {
    Tcp,
    WebSocket,
    Udp,
}
