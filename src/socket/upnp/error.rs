use easy_upnp::Error as UpnpError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UPnPManagerError {
    #[error("UPnP error: {0}")]
    Upnp(#[from] UpnpError),
}
