use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum AutoSelectorError {
    #[error("UPnPNotAvailable error")]
    UPnPNotAvailable,
    #[error("UPnPPortMapFailed error")]
    UPnPPortMapFailed,
    #[error("StunConnectionFailed error")]
    StunConnectionFailed,
    #[error("NoAvailableProxy error")]
    NoAvailableProxy,
    #[error("ServerUnreachable error")]
    ServerUnreachable,
    #[error("NoAvailableMethod error")]
    NoAvailableMethod,
    #[error("Bind error")]
    Bind,
    #[error("Connect error")]
    Connect,
    #[error("Send error")]
    Send,
    #[error("Receive error")]
    Receive,
    #[error("Timeout")]
    Timeout,
    #[error("InvalidResponse {0}")]
    InvalidResponse(String),
    #[error("InvalidHost {0}")]
    InvalidHost(String),
}
