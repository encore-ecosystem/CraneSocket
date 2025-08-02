mod structs;
mod tcp;
mod udp;
mod ws;

pub use {tcp::TcpConnection, udp::UdpConnection, ws::WebSocketConnection};
