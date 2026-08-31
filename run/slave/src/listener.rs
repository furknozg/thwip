use proxy_common::Config;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    io,
    net::{SocketAddr, TcpListener},
};

pub const DEFAULT_BACKLOG: i32 = 1024;

pub struct BoundListener {
    pub socket: TcpListener,
    pub server_index: usize,
}

pub fn bind_worker_listener(address: SocketAddr, backlog: i32) -> io::Result<TcpListener> {
    if backlog <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "listener backlog must be greater than zero",
        ));
    }

    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;

    socket.set_reuse_address(true)?;

    // The one-listener-per-worker model relies on Linux SO_REUSEPORT. Other
    // platforms can still build and exercise the socket factory, but do not
    // provide the project's epoll/io_uring runtime support.
    #[cfg(target_os = "linux")]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;

    let socket_address = SockAddr::from(address);
    socket.bind(&socket_address)?;
    socket.listen(backlog)?;

    Ok(socket.into())
}

pub fn bind_worker_listeners(config: &Config) -> io::Result<Vec<BoundListener>> {
    config
        .http
        .servers
        .iter()
        .enumerate()
        .map(|(server_index, server)| {
            bind_worker_listener(server.listen, DEFAULT_BACKLOG)
                .map(|socket| BoundListener {
                    socket,
                    server_index,
                })
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("failed to bind listener at {}: {error}", server.listen),
                    )
                })
        })
        .collect()
}
