use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use tokio::{net::TcpStream, sync::oneshot::Receiver};
use tokio_tungstenite::{WebSocketStream, accept_async};

use crate::socket::ListenerError;
use crate::socket::upnp::UPnPManager;
use crate::socket::upnp::listeners::ConnectionProtocol;
use crate::socket::upnp::listeners::upnp::init_upnp;

#[allow(dead_code)]
#[derive(Debug)]
pub struct WebSocketListener {
    upnp_manager: Option<UPnPManager>,
    listener: tokio::net::TcpListener,
}

type HandlerFn = fn(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> HandlerFuture;
type HandlerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

impl WebSocketListener {
    pub async fn bind(local_addr: &SocketAddr) -> Result<Self, ListenerError> {
        let listener = tokio::net::TcpListener::bind(&local_addr).await?;
        let upnp_manager =
            init_upnp(listener.local_addr()?.port(), ConnectionProtocol::Tcp).await?;

        Ok(Self {
            upnp_manager: Some(upnp_manager),
            listener,
        })
    }

    pub async fn accept(
        &self,
    ) -> Result<(WebSocketStream<TcpStream>, SocketAddr), tokio_tungstenite::tungstenite::Error>
    {
        let (stream, addr) = self.listener.accept().await?;
        let stream = accept_async(stream).await?;
        Ok((stream, addr))
    }

    pub async fn host(self, handler: HandlerFn, mut shutdown_rx: Receiver<()>) {
        loop {
            tokio::select! {
                conn = async {
                        self.accept().await
                } => {
                    match conn {
                        Ok((stream, addr)) => {
                            log::debug!("Accepted new peer: {}", addr);
                            tokio::spawn(async move {handler(stream).await;});
                        }
                        Err(e) => {
                            log::debug!("Exit accept loop: {}", e);
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
