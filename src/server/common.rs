use crate::socket::{ListenerError, proxy::WebSocketListener};

pub enum Listener {
    WebSocketListener(WebSocketListener),
    UPnPWebSocketListener(crate::socket::upnp::WebSocketListener),
}

impl Listener {
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
            Listener::WebSocketListener(listener) => listener.accept().await,
            Listener::UPnPWebSocketListener(listener) => listener.accept().await,
        }
    }
}
