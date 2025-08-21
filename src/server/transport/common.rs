use std::net::SocketAddr;

use log::{debug, error};
use tokio::sync::broadcast;

use crate::server::constant::MAX_ERRORS_ALLOWED;

pub fn send_max_errors_reached_msg(addr: &SocketAddr, actor_tx: &broadcast::Sender<()>) {
    error!(
        "Max channel errors reached ({}), closing connection, peer_id={}",
        MAX_ERRORS_ALLOWED, addr
    );

    match actor_tx.send(()) {
        Ok(_) => {
            debug!("Successfully notified actors about shutdown")
        }
        Err(e) => {
            debug!("Failed to notify actors about shutdown: {}", e)
        }
    }
}
