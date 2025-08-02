# CraneSocket

## Examples

### UPNP
Code examples provided in <code>examples/upnp</code>. 

Usage:
1. start server with <code>cargo run --bin upnp_server</code>
2. start client with <code>cargo run --bin upnp_client</code>

### Proxy
Code examples provided in <code>examples/proxy</code>. 

0. Specify file path in <code>sender.rs</code>
1. Start proxy server (<code>cargo run --bin proxy_server</code>)
2. Start receiver and connect to the server (<code>cargo run --bin receiver</code>)
3. Start sender and connect to the server (<code>cargo run --bin sender</code>)
4. Wait until data is transfered