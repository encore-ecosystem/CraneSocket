use tokio::net::TcpListener;

use crate::socket::ListenerError;

pub enum Listener {
    TcpListener(TcpListener),
    UPnPTcpListener(crate::socket::upnp::TcpListener),
}

impl Listener {
    pub async fn accept(
        &self,
    ) -> std::result::Result<(tokio::net::TcpStream, std::net::SocketAddr), ListenerError> {
        match self {
            Listener::TcpListener(listener) => listener.accept().await.map_err(ListenerError::Io),
            Listener::UPnPTcpListener(listener) => listener.accept().await,
        }
    }
}
