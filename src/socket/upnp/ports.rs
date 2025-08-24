use easy_upnp::{Ipv4Cidr, UpnpConfig, add_ports, delete_ports};

use easy_upnp::PortMappingProtocol as UPnPPortMappingProtocol;

use crate::socket::utils::ConnectionProtocol;

pub fn get_port_config(port: u16, duration: u32, protocol: ConnectionProtocol) -> UpnpConfig {
    let protocol = match protocol {
        ConnectionProtocol::Tcp => UPnPPortMappingProtocol::TCP,
        ConnectionProtocol::WebSocket => UPnPPortMappingProtocol::TCP,
        ConnectionProtocol::Udp => UPnPPortMappingProtocol::UDP,
    };
    UpnpConfig {
        address: Some(Ipv4Cidr::from_str("192.168.0").unwrap()),
        port,
        protocol,
        duration,
        comment: "Webcrane server".to_string(),
    }
}

pub fn open_port(
    port: u16,
    duration: u32,
    protocol: ConnectionProtocol,
) -> Result<(), easy_upnp::Error> {
    add_ports([get_port_config(port, duration, protocol)])
        .next()
        .unwrap()
}

pub fn close_port(port: u16, protocol: ConnectionProtocol) -> Result<(), easy_upnp::Error> {
    let duration = 0;
    delete_ports([get_port_config(port, duration, protocol)])
        .next()
        .unwrap()
}
