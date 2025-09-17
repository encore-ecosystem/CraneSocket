use std::time::Duration;

use crate::socket::{
    upnp::ports::{close_port, open_port},
    utils::ConnectionProtocol,
};

mod error;

mod ports;
mod tcp;
mod udp;
mod utils;
mod ws;

pub use error::*;
pub use tcp::{TcpConnection, TcpListener};
pub use udp::{UdpConnection, UdpListener};
pub use utils::*;
pub use ws::{WebSocketConnection, WebSocketListener};

#[derive(Debug)]
pub struct UPnPManager {
    port: u16,
    protocol: ConnectionProtocol,
    prolongation_task: tokio::task::JoinHandle<()>,
}

impl UPnPManager {
    pub fn new(port: u16, protocol: ConnectionProtocol) -> Result<Self, UPnPManagerError> {
        log::debug!("Opening UPnP port {}...", port);

        let duration = 600;
        open_port(port, duration, protocol)?;

        // background task for port prolongation
        let prolongation_task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let interval_duration = std::cmp::max(1, duration / 2) as u64;
            let mut interval = tokio::time::interval(Duration::from_secs(interval_duration));
            loop {
                interval.tick().await;
                match open_port(port, duration, protocol) {
                    Ok(()) => log::debug!("Renewed UPnP port mapping for port {}", &port),
                    Err(e) => {
                        log::error!("Failed to renew port. Retrying... {}", e);
                        interval.reset();
                        tokio::time::sleep(Duration::from_secs((duration / 100) as u64)).await;
                    }
                }
            }
        });

        log::debug!("Successfully opened port {} for {} seconds", port, duration);

        Ok(UPnPManager {
            port,
            protocol,
            prolongation_task,
        })
    }
}

impl Drop for UPnPManager {
    fn drop(&mut self) {
        log::debug!("Closing UPnP port {}...", self.port);
        self.prolongation_task.abort();
        close_port(self.port, self.protocol).unwrap();
        log::debug!("Successfully closed UPnP port {}", self.port);
    }
}
