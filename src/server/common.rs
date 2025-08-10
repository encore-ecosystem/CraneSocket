use std::net::SocketAddr;

use crate::socket::{ListenerError, proxy::WebSocketListener as ProxyWebSocketListener};

pub enum WebSocketListener {
    ProxyWebSocketListener(ProxyWebSocketListener),
    UPnPWebSocketListener(crate::socket::upnp::WebSocketListener),
}

impl WebSocketListener {
    pub async fn accept(
        &self,
    ) -> std::result::Result<
        (
            tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
            std::net::SocketAddr,
        ),
        ListenerError,
    > {
        match self {
            WebSocketListener::ProxyWebSocketListener(listener) => listener.accept().await,
            WebSocketListener::UPnPWebSocketListener(listener) => listener.accept().await,
        }
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        match self {
            WebSocketListener::ProxyWebSocketListener(listener) => listener.get_local_addr(),
            WebSocketListener::UPnPWebSocketListener(listener) => listener.get_local_addr(),
        }
    }
}
