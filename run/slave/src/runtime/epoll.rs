use super::{Runtime, ShutdownHandle, WorkerContext, WorkerLimits};
use crate::BoundListenerGroup;
#[cfg(unix)]
use crate::{
    parse_request_head, response_bytes, route, select_server, static_response_bytes, RequestHead,
    RequestHeadParse,
};
#[cfg(unix)]
use mio::{net::TcpListener, net::TcpStream, Events, Interest, Poll, Token};
#[cfg(unix)]
use proxy_common::Action;
use proxy_common::Server;
#[cfg(unix)]
use slab::Slab;
use std::io;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
const MAX_READS_PER_EVENT: usize = 16;
#[cfg(unix)]
const MAX_WRITES_PER_EVENT: usize = 16;

pub struct EpollRuntime {
    pub max_events: usize,
}

#[cfg(unix)]
struct Connection {
    socket: TcpStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    write_offset: usize,
    request_head_complete: bool,
    listener_group: usize,
    last_progress: Instant,
}

#[cfg(unix)]
struct EpollWorker {
    poll: Poll,
    listeners: Vec<RegisteredListener>,
    connections: Slab<Connection>,
    servers: Vec<Server>,
    shutdown: ShutdownHandle,
    limits: WorkerLimits,
    draining: bool,
    drain_started_at: Option<Instant>,
}

#[cfg(unix)]
struct RegisteredListener {
    socket: TcpListener,
    default_server: usize,
    server_indices: Vec<usize>,
}

#[cfg(unix)]
impl EpollWorker {
    fn new(context: WorkerContext) -> io::Result<Self> {
        let WorkerContext {
            listener_groups,
            servers,
            shutdown,
            limits,
        } = context;
        let poll = Poll::new()?;
        let mut listeners: Vec<RegisteredListener> = listener_groups
            .into_iter()
            .map(|listener| RegisteredListener {
                socket: TcpListener::from_std(listener.socket),
                default_server: listener.default_server,
                server_indices: listener.server_indices,
            })
            .collect();

        for (index, listener) in listeners.iter_mut().enumerate() {
            poll.registry()
                .register(&mut listener.socket, Token(index), Interest::READABLE)?;
        }

        Ok(Self {
            poll,
            listeners,
            connections: Slab::new(),
            servers,
            shutdown,
            limits,
            draining: false,
            drain_started_at: None,
        })
    }

    fn run(mut self, max_events: usize) -> io::Result<()> {
        let mut events = Events::with_capacity(max_events.max(1));

        loop {
            if self.shutdown.is_requested() && !self.draining {
                self.begin_shutdown()?;
            }
            if self.draining && self.connections.is_empty() {
                return Ok(());
            }
            if self.drain_deadline_expired() {
                self.close_all_connections();
                return Ok(());
            }
            self.close_idle_connections()?;

            self.poll
                .poll(&mut events, Some(Duration::from_millis(100)))?;
            let ready: Vec<(Token, bool, bool, bool, bool, bool)> = events
                .iter()
                .map(|event| {
                    (
                        event.token(),
                        event.is_readable(),
                        event.is_writable(),
                        event.is_error(),
                        event.is_read_closed(),
                        event.is_write_closed(),
                    )
                })
                .collect();

            for (token, readable, writable, error, read_closed, write_closed) in ready {
                if token.0 < self.listeners.len() {
                    if readable && !self.draining {
                        self.accept_ready(token.0)?;
                    }
                } else {
                    let connection_id = token.0 - self.listeners.len();
                    if error || write_closed {
                        self.remove_connection(connection_id)?;
                        continue;
                    }
                    if writable {
                        self.write_ready(connection_id)?;
                    }
                    if readable && !self.draining {
                        self.connection_ready(connection_id)?;
                    }
                    if read_closed && self.connections.contains(connection_id) {
                        let connection = &self.connections[connection_id];
                        if connection.write_offset == connection.write_buffer.len() {
                            self.remove_connection(connection_id)?;
                        }
                    }
                }
            }
        }
    }

    fn accept_ready(&mut self, listener_index: usize) -> io::Result<()> {
        loop {
            match self.listeners[listener_index].socket.accept() {
                Ok((stream, peer_address)) => {
                    if self.connections.len() >= self.limits.max_connections {
                        eprintln!("connection limit reached; dropping {peer_address}");
                        continue;
                    }
                    println!("accepted connection from {peer_address}");
                    self.register_connection(stream, listener_index)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    eprintln!("accept failed on listener {listener_index}: {error}");
                    return Ok(());
                }
            }
        }
    }

    fn register_connection(&mut self, stream: TcpStream, listener_group: usize) -> io::Result<()> {
        let connection_id = self.connections.insert(Connection {
            socket: stream,
            read_buffer: Vec::with_capacity(8 * 1024),
            write_buffer: Vec::new(),
            write_offset: 0,
            request_head_complete: false,
            listener_group,
            last_progress: Instant::now(),
        });
        let token = Token(self.listeners.len() + connection_id);

        self.poll.registry().register(
            &mut self.connections[connection_id].socket,
            token,
            Interest::READABLE,
        )
    }

    fn connection_ready(&mut self, connection_id: usize) -> io::Result<()> {
        if !self.connections.contains(connection_id) {
            return Ok(());
        }

        let mut close_connection = false;
        let mut request_too_large = false;
        let mut request: Option<RequestHead> = None;
        let mut listener_group = 0;
        {
            use std::io::Read;

            let connection = &mut self.connections[connection_id];
            let mut buffer = [0_u8; 8 * 1024];

            for _ in 0..MAX_READS_PER_EVENT {
                match connection.socket.read(&mut buffer) {
                    Ok(0) => {
                        close_connection = true;
                        break;
                    }
                    Ok(read) => {
                        if connection.read_buffer.len() + read > self.limits.max_read_buffer_size {
                            request_too_large = true;
                            connection.request_head_complete = true;
                            break;
                        }
                        connection.read_buffer.extend_from_slice(&buffer[..read]);
                        connection.last_progress = Instant::now();

                        if !connection.request_head_complete {
                            match parse_request_head(&connection.read_buffer) {
                                Ok(RequestHeadParse::Incomplete) => {}
                                Ok(RequestHeadParse::Complete {
                                    request: request_head,
                                    ..
                                }) => {
                                    connection.request_head_complete = true;
                                    println!("{} {}", request_head.method, request_head.target);
                                    listener_group = connection.listener_group;
                                    request = Some(request_head);
                                    break;
                                }
                                Err(error) => {
                                    eprintln!("invalid HTTP request: {error}");
                                    close_connection = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        eprintln!("connection read failed: {error}");
                        close_connection = true;
                        break;
                    }
                }
            }
        }

        if request_too_large {
            self.queue_response(connection_id, response_bytes(413, "request is too large"))?;
        } else if let Some(request) = request {
            let Some(listener) = self.listeners.get(listener_group) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "listener group is unavailable",
                ));
            };
            let server_index = select_server(
                &listener.server_indices,
                listener.default_server,
                &request,
                &self.servers,
            );
            let response = self.response_for(server_index, &request.method, &request.target);
            self.queue_response(connection_id, response)?;
        }

        if close_connection {
            self.remove_connection(connection_id)?;
        }

        Ok(())
    }

    fn write_ready(&mut self, connection_id: usize) -> io::Result<()> {
        if !self.connections.contains(connection_id) {
            return Ok(());
        }

        let mut write_failed = false;
        {
            use std::io::Write;

            let connection = &mut self.connections[connection_id];
            for _ in 0..MAX_WRITES_PER_EVENT {
                if connection.write_offset == connection.write_buffer.len() {
                    break;
                }
                match connection
                    .socket
                    .write(&connection.write_buffer[connection.write_offset..])
                {
                    Ok(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "socket write returned zero",
                        ))
                    }
                    Ok(written) => {
                        connection.write_offset += written;
                        connection.last_progress = Instant::now();
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => {
                        eprintln!("connection write failed: {error}");
                        write_failed = true;
                        break;
                    }
                }
            }
        }

        if write_failed {
            self.remove_connection(connection_id)?;
        } else if self.connections.contains(connection_id)
            && self.connections[connection_id].write_offset
                == self.connections[connection_id].write_buffer.len()
        {
            self.remove_connection(connection_id)?;
        }

        Ok(())
    }

    fn response_for(&self, server_index: usize, method: &str, target: &str) -> Vec<u8> {
        let Some(server) = self.servers.get(server_index) else {
            return response_bytes(500, "server configuration is unavailable");
        };

        match route(server, target) {
            Some(Action::Response { status, body }) => response_bytes(*status, body),
            Some(Action::Static { directory }) => {
                static_response_bytes(directory.as_ref(), method, target)
            }
            Some(Action::Proxy { .. }) => {
                response_bytes(501, "configured action is not implemented")
            }
            None => response_bytes(404, "not found"),
        }
    }

    fn begin_shutdown(&mut self) -> io::Result<()> {
        self.draining = true;
        self.drain_started_at = Some(Instant::now());
        for listener in &mut self.listeners {
            if let Err(error) = self.poll.registry().deregister(&mut listener.socket) {
                eprintln!("failed to deregister listener during shutdown: {error}");
            }
        }

        // Preserve only responses already queued for writing. There is no
        // keep-alive yet, so all read-only connections can close immediately.
        let close_ids: Vec<usize> = self
            .connections
            .iter()
            .filter_map(|(id, connection)| {
                (connection.write_offset >= connection.write_buffer.len()).then_some(id)
            })
            .collect();
        for connection_id in close_ids {
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn remove_connection(&mut self, connection_id: usize) -> io::Result<()> {
        if self.connections.contains(connection_id) {
            let mut connection = self.connections.remove(connection_id);
            if let Err(error) = self.poll.registry().deregister(&mut connection.socket) {
                eprintln!("failed to deregister connection: {error}");
            }
        }
        Ok(())
    }

    fn queue_response(&mut self, connection_id: usize, mut response: Vec<u8>) -> io::Result<()> {
        if !self.connections.contains(connection_id) {
            return Ok(());
        }
        if response.len() > self.limits.max_write_buffer_size {
            response = response_bytes(500, "response is too large");
        }

        let connection = &mut self.connections[connection_id];
        connection.write_buffer = response;
        connection.write_offset = 0;
        self.poll.registry().reregister(
            &mut connection.socket,
            Token(self.listeners.len() + connection_id),
            Interest::WRITABLE,
        )
    }

    fn close_idle_connections(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let expired: Vec<usize> = self
            .connections
            .iter()
            .filter_map(|(id, connection)| {
                (now.duration_since(connection.last_progress) >= self.limits.idle_timeout)
                    .then_some(id)
            })
            .collect();
        for connection_id in expired {
            eprintln!("connection {connection_id} timed out");
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn drain_deadline_expired(&self) -> bool {
        self.drain_started_at
            .is_some_and(|started| started.elapsed() >= self.limits.drain_timeout)
    }

    fn close_all_connections(&mut self) {
        let ids: Vec<usize> = self.connections.iter().map(|(id, _)| id).collect();
        for connection_id in ids {
            if let Err(error) = self.remove_connection(connection_id) {
                eprintln!("failed to close connection during shutdown: {error}");
            }
        }
    }
}

impl Runtime for EpollRuntime {
    fn run(self, context: WorkerContext) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            run_readiness(context, self.max_events)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = context;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "epoll is only supported on Linux",
            ))
        }
    }
}

#[cfg(unix)]
pub fn run_epoll(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
) -> io::Result<()> {
    run_epoll_with_shutdown(listener_groups, servers, max_events, ShutdownHandle::new())
}

#[cfg(unix)]
pub fn run_epoll_with_shutdown(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
    shutdown: ShutdownHandle,
) -> io::Result<()> {
    EpollRuntime { max_events }.run(WorkerContext {
        listener_groups,
        servers,
        shutdown,
        limits: WorkerLimits::default(),
    })
}

#[cfg(unix)]
pub(crate) fn run_readiness(context: WorkerContext, max_events: usize) -> io::Result<()> {
    EpollWorker::new(context)?.run(max_events)
}

#[cfg(not(unix))]
pub fn run_epoll(
    _listener_groups: Vec<BoundListenerGroup>,
    _servers: Vec<Server>,
    _max_events: usize,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "epoll is only supported on Linux",
    ))
}

#[cfg(not(unix))]
pub fn run_epoll_with_shutdown(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
    _shutdown: ShutdownHandle,
) -> io::Result<()> {
    run_epoll(listener_groups, servers, max_events)
}
