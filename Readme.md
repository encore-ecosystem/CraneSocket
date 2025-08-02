# ws-proxy-server

**Version:** 0.1.0  
**License:** MIT (or specify your preferred license)  

`ws-proxy-server` is a lightweight Rust library for building a WebSocket proxy server with support for multiple asynchronous tasks. It provides a robust framework for handling WebSocket connections, processing client and server messages, and managing graceful shutdowns. The library is built using Tokio for asynchronous I/O and includes features like message filtering, error handling, and notification of client disconnections.

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