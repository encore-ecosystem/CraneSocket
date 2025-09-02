use crane_socket::{
    server::message::{ClientMessage, ServerMessage},
    socket::proxy::WebSocketConnection,
};
use log::{debug, error};
use tokio::{fs::File, io::AsyncReadExt, sync::oneshot};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let socket = WebSocketConnection::from_token("AQEAfwAAAcA+AQhVVEVxZzN1Ug==")
        .await
        .unwrap();

    let (mut sink, mut stream) = socket.split();

    let (tx, mut rx) = oneshot::channel();

    tokio::spawn(async move {
        tokio::select! {
            Ok(msg) = stream.next() => {
                match msg {
                    ServerMessage::ClientLeft | ServerMessage::Close(_) => {
                        debug!("Connection closed");
                        tx.send(123).unwrap();
                    }
                    _ => {}
                }
            }
        }
    });

    let file_path = "/home/conk/Files/Programming/123.txt";
    let mut file = File::open(file_path).await.unwrap();

    let file_size = file.metadata().await.unwrap().len();
    println!("{:?}", file_size);
    sink.send(ClientMessage::text(file_size.to_string()))
        .await
        .unwrap();

    let mut buffer = vec![0u8; 1024 * 1024];
    let mut total_sent = 0;
    loop {
        let bytes_read = file.read(&mut buffer).await.unwrap();
        if bytes_read == 0 {
            break;
        }
        if rx.try_recv().is_ok() {
            sink.send(ClientMessage::Close(None)).await.unwrap();
            return;
        }

        match sink
            .send(ClientMessage::binary(buffer[..bytes_read].to_vec()))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                break;
            }
        }
        total_sent += bytes_read as u64;
        debug!("total bytes sent: {}", total_sent);
    }
    sink.send(ClientMessage::Close(None)).await.unwrap();

    println!(
        "File {} has been successfully sent ({} bytes)",
        file_path, total_sent
    );
}
