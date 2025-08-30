use dotenv::dotenv;
use std::net::SocketAddr;
use crane_socket::socket::upnp::UdpConnection;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let socket = UdpConnection::bind(&"0.0.0.0:0".parse().unwrap())
        .await
        .unwrap();
    socket
        .connect(&"127.0.0.1:8080".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();

    let msg = b"Hello!";
    socket.send(msg).await.unwrap();
    println!("Sent: {:?}", String::from_utf8_lossy(msg));

    let mut buf = [0u8; 1024];
    let len = socket.recv(&mut buf).await.unwrap();
    println!("Received: {:.?}", String::from_utf8_lossy(&buf[..len]));
}
