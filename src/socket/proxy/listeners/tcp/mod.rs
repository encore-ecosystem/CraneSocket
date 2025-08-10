use std::net::{Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::socket::ListenerError;

mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TcpListener {
    listener: tokio::net::TcpListener,
}

impl TcpListener {
    pub async fn bind(local_addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&local_addr).await?;

        Ok(TcpListener { listener })
    }

    pub async fn listen(
        server_addr: &SocketAddr,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ListenerError> {
        // let (mut server_conn, _response) = connect_async(format!("ws://{}", server_addr)).await?;
        // debug!("Connected to proxy server");

        // let room_id = register(&mut server_conn).await?;
        // debug!("Created a room on proxy server");

        // println!("\n\n{}\n\n", room_id);

        // wait_for_another_peer(&mut server_conn).await?;
        // debug!("Another peer successfully connected to proxy server");

        // Ok(server_conn)

        unimplemented!()
    }

    pub async fn accept(&self) -> Result<(TcpStream, SocketAddr), ListenerError> {
        Ok(self.listener.accept().await?)
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn get_external_addr(&self) -> Result<Ipv4Addr, ListenerError> {
        match external_ip::get_ipv4().await {
            Some(addr) => Ok(addr),
            None => Err(ListenerError::Socket("failed to get external IP".into())),
        }
    }

    pub async fn get_info() {}
}
