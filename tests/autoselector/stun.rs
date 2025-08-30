use crane_socket::socket::{
    common::STUN_HOSTS,
    utils::{
        AutoSelectorConfig, AutoSelectorError, ConnectionProtocol, ListeningMethod,
        auto_select_conn_method,
    },
};

#[tokio::test]
async fn test_autoselector_stun_success() {
    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .stun_servers(STUN_HOSTS.to_vec());
    let method = auto_select_conn_method(&cfg).await.unwrap();

    assert_eq!(method, ListeningMethod::Stun(STUN_HOSTS[0].to_owned()));
}

#[tokio::test]
async fn test_autoselector_stun_invalid_host() {
    let cfg = AutoSelectorConfig::new(ConnectionProtocol::Udp)
        .upnp(false)
        .stun_servers(Vec::from(["123"]));
    let method = auto_select_conn_method(&cfg).await.err();

    if let Some(err) = method {
        assert_eq!(err, AutoSelectorError::NoAvailableMethod)
    } else {
        panic!("Expected AutoSelctorError, got None")
    }
}
