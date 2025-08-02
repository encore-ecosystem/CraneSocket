mod structs;
mod tcp;
mod udp;
mod websocket;

pub use {tcp::TcpConnection, udp::UdpConnection, websocket::WebSocketConnection};
