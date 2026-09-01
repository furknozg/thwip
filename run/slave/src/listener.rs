use proxy_common::Config;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    io,
    net::{SocketAddr, TcpListener},
};

pub const DEFAULT_BACKLOG: i32 = 1024;

struct ListenerGroup {
    address: SocketAddr,
    default_server: usize,
    server_indices: Vec<usize>,
}

pub struct BoundListenerGroup {
    pub socket: TcpListener,
    pub address: SocketAddr,
    pub default_server: usize,
    pub server_indices: Vec<usize>,
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
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;

    let socket_address = SockAddr::from(address);
    socket.bind(&socket_address)?;
    socket.listen(backlog)?;

    Ok(socket.into())
}

pub fn bind_worker_listeners(config: &Config) -> io::Result<Vec<BoundListenerGroup>> {
    let mut groups = Vec::<ListenerGroup>::new();

    for (server_index, server) in config.http.servers.iter().enumerate() {
        let group_index = match groups
            .iter()
            .position(|group| group.address == server.listen)
        {
            Some(index) => index,
            None => {
                groups.push(ListenerGroup {
                    address: server.listen,
                    default_server: server_index,
                    server_indices: Vec::new(),
                });
                groups.len() - 1
            }
        };

        groups[group_index].server_indices.push(server_index);
    }

    groups
        .into_iter()
        .map(|group| -> io::Result<BoundListenerGroup> {
            let socket = bind_worker_listener(group.address, DEFAULT_BACKLOG)?;

            Ok(BoundListenerGroup {
                socket,
                address: group.address,
                default_server: group.default_server,
                server_indices: group.server_indices,
            })
        })
        .collect()
}
