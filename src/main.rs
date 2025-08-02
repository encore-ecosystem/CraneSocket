use dotenv::dotenv;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async;

use crate::socket::get_default_gateway;
use crate::socket::upnp::TcpListener;

pub mod config;
pub mod server;
pub mod socket;

async fn accept_connection(peer: SocketAddr, stream: TcpStream) {
    let ws_stream = accept_async(stream).await.unwrap();
    let (mut _writer, mut _reader) = ws_stream.split();
    log::info!("Accepted {}", peer.ip());
}

fn accept_connection_wrapper(
    peer: SocketAddr,
    stream: TcpStream,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(accept_connection(peer, stream))
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let local_addr = get_default_gateway().await.unwrap().to_string() + ":" + "4321";

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        log::info!("Ctrl+C received, shutting down...");
        let _ = shutdown_tx.send(());
    });

    let listener = TcpListener::bind(&local_addr.parse().unwrap())
        .await
        .unwrap();
    log::info!("Server listening on {}", local_addr);

    listener.host(accept_connection_wrapper, shutdown_rx).await;
}
