use crane_socket::socket::upnp::WebSocketListener;
use dotenv::dotenv;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let addr = "127.0.0.1:8000".parse().unwrap();
    let listener = WebSocketListener::listen(&addr).await.unwrap();

    let (stream, _) = listener.accept().await.unwrap();

    let (_, mut recieiver) = stream.split();

    if let Ok(Message::Binary(data)) = recieiver.next().await {
        println!("decrypted buf: {:?}", &data[..]);
    } else {
        panic!("expected binary msg");
    }
}
