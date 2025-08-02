use log::debug;
use std::net::SocketAddr;
use tokio_tungstenite::connect_async;

use crate::socket::{
    ListenerError,
    proxy::{
        WebSocketConnection,
        listeners::ws::utils::{register, wait_for_another_peer},
    },
};

mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct WebSocketListener {
    listener: tokio::net::TcpListener,
}

impl WebSocketListener {
    pub async fn listen(server_addr: &SocketAddr) -> Result<WebSocketConnection, ListenerError> {
        let (mut server_conn, _response) = connect_async(format!("ws://{}", server_addr)).await?;
        debug!("Connected to proxy server");

        let room_id = register(&mut server_conn).await?;
        debug!("Created a room on proxy server");

        println!("\n\n{}\n\n", room_id);

        wait_for_another_peer(&mut server_conn).await?;
        debug!("Another peer successfully connected to proxy server");

        Ok(WebSocketConnection::new(server_conn))
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }
}
