use tokio::net::UdpSocket;

pub async fn get_default_gateway() -> std::io::Result<std::net::IpAddr> {
    let remote = "8.8.8.8:80";
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(remote).await?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip())
}
