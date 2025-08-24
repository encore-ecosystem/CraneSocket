use crate::socket::{ListenerError, upnp::UPnPManager, utils::ConnectionProtocol};

pub async fn init_upnp(
    port: u16,
    protocol: ConnectionProtocol,
) -> Result<UPnPManager, ListenerError> {
    let upnp_manager = UPnPManager::new(port, protocol)?;
    Ok(upnp_manager)
}
