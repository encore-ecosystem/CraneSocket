use dotenv::dotenv;
use log::info;
use tokio::{signal, sync::oneshot};
use upnpsocket::socket::upnp::UdpListener;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let socket = UdpListener::bind(&"0.0.0.0:8080".parse().unwrap())
        .await
        .unwrap();
    info!("Server started");
    info!("Server ext addr: {}", socket.get_external_addr());

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Ctrl+C received, shutting down...");
        let _ = shutdown_tx.send(());
    });

    let mut buf = [0u8; 1024];
    loop {
        tokio::select! {
            conn = async {
                    socket.recv_from(&mut buf).await
            } => {
                match conn {
                    Ok((len, addr)) => {
                        println!("Received {:?} from {}", String::from_utf8_lossy(&buf[..len]), addr);
                        socket.send_to(&buf[..len], &addr).await.unwrap();
                    },
                    Err(e) => {
                        log::info!("Exit accept loop: {}", e);
                        break;
                    }
                }

            },
            _ = &mut shutdown_rx => {
                info!("Shutdown signal received.");
                break;
            }
        }
    }
}
