use dotenv::dotenv;
use tokio::{signal, sync::oneshot};
use upnpsocket::server::WebSocketProxyServer;

#[tokio::main(flavor = "multi_thread", worker_threads = 12)]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = WebSocketProxyServer::bind(&addr, false).await.unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        let _ = shutdown_tx.send(());
    });
    server.serve(shutdown_rx).await.unwrap();
}
