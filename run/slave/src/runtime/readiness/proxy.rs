#![cfg(unix)]

use mio::net::TcpStream;

/// Owns the upstream half of one proxied client connection. Request and
/// response offsets stay here because they advance independently of the client.
pub(super) struct ProxyState {
    pub(super) upstream: TcpStream,
    pub(super) request_buffer: Vec<u8>,
    pub(super) request_offset: usize,
    pub(super) upstream_eof: bool,
}
