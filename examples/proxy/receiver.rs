use log::debug;
use tokio::{fs::File, io::AsyncWriteExt};

use crane_socket::{server::message::ServerMessage, socket::proxy::WebSocketConnection};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let server_addr = "127.0.0.1:8000".parse().unwrap();
    log::debug!("Signaling server addr: {}", server_addr);
    let (mut ws_stream, room_id) = WebSocketConnection::create_room(&server_addr)
        .await
        .unwrap();

    println!("Room ID: {}", room_id);
    println!("Token: {}", ws_stream.get_token().unwrap());

    ws_stream.wait_for_client().await.unwrap();

    let msg = ws_stream.next().await.unwrap();

    let file_size = msg.to_string().parse::<u64>().unwrap();
    println!("File size: {} bytes", file_size);

    let file_name = "/home/conk/Files/Programming/received_file.gguf";
    let mut file = File::create(&file_name).await.unwrap();
    let mut total_received = 0;

    while total_received < file_size {
        let msg = ws_stream.next().await;
        match msg {
            Ok(ServerMessage::Binary(data)) => {
                file.write_all(&data).await.unwrap();
                total_received += data.len() as u64;
                debug!(
                    "Received {} bytes. Total bytes received: {}",
                    data.len(),
                    total_received
                );
            }
            Ok(ServerMessage::Close(_) | ServerMessage::ClientLeft) => {
                println!("Connection closed");
                ws_stream.close().await.unwrap();
                file.flush().await.unwrap();
                return;
            }
            Ok(msg) => {
                eprintln!("Received unexpected message: {:?}", msg);
                ws_stream.close().await.unwrap();
                file.flush().await.unwrap();
                return;
            }
            Err(e) => {
                println!("{}", e);
                ws_stream.close().await.unwrap();
                file.flush().await.unwrap();
                return;
            }
        }
    }

    file.flush().await.unwrap();
    println!(
        "Files {} successfully received ({} bytes)",
        file_name, total_received
    );

    ws_stream.close().await.unwrap();
}
