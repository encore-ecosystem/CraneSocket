use crane_socket::socket::upnp::WebSocketConnection;
use dotenv::dotenv;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let server_addr = "127.0.0.1:8000".parse().unwrap();
    let stream = WebSocketConnection::connect(&server_addr).await.unwrap();

    let (mut sender, _) = stream.split();

    let msg = [1, 2, 3];
    sender.send(Message::binary(msg.to_vec())).await.unwrap();
}
