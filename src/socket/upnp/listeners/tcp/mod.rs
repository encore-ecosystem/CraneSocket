use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use tokio::{net::TcpStream, sync::oneshot::Receiver};

use crate::socket::ListenerError;
use crate::socket::upnp::UPnPManager;
use crate::socket::upnp::listeners::ConnectionProtocol;
use crate::socket::upnp::listeners::upnp::init_upnp;

mod utils;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TcpListener {
    upnp_manager: Option<UPnPManager>,
    listener: tokio::net::TcpListener,
}

type HandlerFn = fn(SocketAddr, TcpStream) -> HandlerFuture;
type HandlerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

impl TcpListener {
    pub async fn bind(local_addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&local_addr).await?;
        let upnp_manager =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Tcp).await?;

        Ok(TcpListener {
            upnp_manager: Some(upnp_manager),
            listener,
        })
    }

    pub async fn accept(&self) -> Result<(TcpStream, SocketAddr), ListenerError> {
        Ok(self.listener.accept().await?)
    }

    pub async fn host(self, handler: HandlerFn, mut shutdown_rx: Receiver<()>) {
        loop {
            tokio::select! {
                conn = async {
                        self.accept().await
                } => {
                    match conn {
                        Ok((stream, _)) => {
                            let peer = stream.peer_addr().expect("Connected streams must have a peer address");
                            log::debug!("Accepted new peer: {}", peer);
                            tokio::spawn(async move {handler(peer, stream).await;});
                        }
                        Err(e) => {
                            log::debug!("Exited accept loop: {}", e);
                            break;
                        }
                    }
                },
                _ = &mut shutdown_rx => {
                    log::debug!("Shutdown signal received. Shutting down...");
                    break;
                }
            }
        }
    }

    pub fn get_local_addr(&self) -> Result<SocketAddr, ListenerError> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn get_external_addr(&self) -> Result<Ipv4Addr, ListenerError> {
        match external_ip::get_ipv4().await {
            Some(addr) => Ok(addr),
            None => Err(ListenerError::Socket("failed to get external IP".into())),
        }
    }
}
