use std::net::SocketAddr;

use tokio::net::TcpStream;

use crate::server::Rooms;
use crate::server::message::ServerMessage;
use crate::socket::ListenerError;
use crate::socket::proxy::TcpListener as ProxyTcpListener;
use crate::socket::proxy::WebSocketListener as ProxyWebSocketListener;
use crate::socket::upnp::TcpListener as UPnPTcpListener;
use crate::socket::upnp::WebSocketListener as UPnPWebSocketListener;

pub enum WebSocketListener {
    ProxyWebSocketListener(ProxyWebSocketListener),
    UPnPWebSocketListener(UPnPWebSocketListener),
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

pub enum TcpListener {
    ProxyTcpListener(ProxyTcpListener),
    UPnPTcpListener(UPnPTcpListener),
}

impl TcpListener {
    pub async fn accept(
        &self,
    ) -> std::result::Result<(TcpStream, std::net::SocketAddr), ListenerError> {
        match self {
            TcpListener::ProxyTcpListener(listener) => listener.accept().await,
            TcpListener::UPnPTcpListener(listener) => listener.accept().await,
        }
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        match self {
            TcpListener::ProxyTcpListener(listener) => listener.get_local_addr(),
            TcpListener::UPnPTcpListener(listener) => listener.get_local_addr(),
        }
    }
}

pub async fn close_server(rooms: Rooms) {
    let rooms_lock = rooms.read().await;
    for room in rooms_lock.values() {
        for (client_addr, sender) in room.iter() {
            sender
                .send((ServerMessage::ServerClosed, *client_addr))
                .await
                .unwrap()
        }
    }
}
