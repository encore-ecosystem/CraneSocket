use std::net::SocketAddr;
use tokio::net::UdpSocket;

use crate::socket::{StunError, upnp::stun::get_external_addr};

pub async fn init_with_stun(socket: &UdpSocket) -> Result<SocketAddr, StunError> {
    let external_addr = get_external_addr(socket, None).await?;

    Ok(external_addr)
}
