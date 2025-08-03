use dotenv::dotenv;
use log::{debug, error};
use tokio::{fs::File, io::AsyncReadExt, sync::oneshot};
use upnpsocket::socket::proxy::{Message, WebSocketConnection};

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let socket =
        WebSocketConnection::join_room(&"127.0.0.1:8000".parse().unwrap(), "0HeOFwjN".into())
            .await
            .unwrap();

    let (mut sink, mut stream) = socket.split();

    let (tx, mut rx) = oneshot::channel();

    tokio::spawn(async move {
        tokio::select! {
            Ok(msg) = stream.next() => {
                match msg {
                    Message::PeerDisconnected | Message::Close(_) => {
                        debug!("Connection closed");
                        tx.send(123).unwrap();
                    }
                    _ => {}
                }
            }
        }
    });

    let file_path = "/home/conk/Files/Programming/gemma-3-1b-it-Q4_K_M.gguf";
    let mut file = File::open(file_path).await.unwrap();

    let file_size = file.metadata().await.unwrap().len();
    println!("{:?}", file_size);
    sink.send(Message::text(file_size.to_string()))
        .await
        .unwrap();

    let mut buffer = vec![0u8; 4096];
    let mut total_sent = 0;
    loop {
        let bytes_read = file.read(&mut buffer).await.unwrap();
        if bytes_read == 0 {
            break;
        }
        if rx.try_recv().is_ok() {
            sink.send(Message::Close(None)).await.unwrap();
            return;
        }

        match sink
            .send(Message::binary(buffer[..bytes_read].to_vec()))
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
    sink.send(Message::Close(None)).await.unwrap();

    // println!(
    //     "File {} has been successfully sent ({} bytes)",
    //     file_path, total_sent
    // );
}
