use super::{Runtime, ShutdownHandle, WorkerContext, WorkerLimits};
use crate::BoundListenerGroup;
#[cfg(unix)]
use crate::{
    parse_request_head, response_bytes, route, select_server, static_response_bytes,
    BodyFramingError, RequestHead, RequestHeadParse,
};
#[cfg(unix)]
use mio::{net::TcpListener, net::TcpStream, Events, Interest, Poll, Token, Waker};
#[cfg(unix)]
use proxy_common::Action;
use proxy_common::Server;
#[cfg(unix)]
use slab::Slab;
#[cfg(unix)]
use std::collections::HashSet;
use std::io;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
const MAX_READS_PER_EVENT: usize = 16;
#[cfg(unix)]
const MAX_WRITES_PER_EVENT: usize = 16;
#[cfg(unix)]
const CONTROL_TOKEN: Token = Token(usize::MAX);
#[cfg(unix)]
const CONNECTION_TAG: usize = 1 << (usize::BITS - 1);
#[cfg(unix)]
const SLOT_BITS: u32 = usize::BITS / 2;
#[cfg(unix)]
const SLOT_MASK: usize = (1usize << SLOT_BITS) - 1;
#[cfg(unix)]
const GENERATION_BITS: u32 = usize::BITS - SLOT_BITS - 1;
#[cfg(unix)]
const GENERATION_MASK: usize = (1usize << GENERATION_BITS) - 1;

pub struct EpollRuntime {
    pub max_events: usize,
}

#[cfg(unix)]
struct Connection {
    socket: TcpStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    write_offset: usize,
    pending_request: Option<PendingRequest>,
    listener_group: usize,
    last_progress: Instant,
    generation: usize,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ConnectionId {
    slot: usize,
    generation: usize,
}

#[cfg(unix)]
impl ConnectionId {
    fn token(self) -> io::Result<Token> {
        if self.slot > SLOT_MASK || self.generation == 0 || self.generation > GENERATION_MASK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection identifier cannot be encoded as a mio token",
            ));
        }
        let token = Token(CONNECTION_TAG | (self.generation << SLOT_BITS) | self.slot);
        if token == CONTROL_TOKEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection identifier collides with the control token",
            ));
        }
        Ok(token)
    }

    fn from_token(token: Token) -> Option<Self> {
        if token == CONTROL_TOKEN || token.0 & CONNECTION_TAG == 0 {
            return None;
        }
        let generation = (token.0 >> SLOT_BITS) & GENERATION_MASK;
        (generation != 0).then_some(Self {
            slot: token.0 & SLOT_MASK,
            generation,
        })
    }
}

#[cfg(unix)]
struct PendingRequest {
    head: RequestHead,
    body_end: usize,
}

#[cfg(unix)]
struct EpollWorker {
    poll: Poll,
    listeners: Vec<RegisteredListener>,
    connections: Slab<Connection>,
    generations: Vec<usize>,
    pending_writes: HashSet<ConnectionId>,
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
    fn is_current(&self, connection_id: ConnectionId) -> bool {
        self.connections
            .get(connection_id.slot)
            .is_some_and(|connection| connection.generation == connection_id.generation)
    }

    fn log_socket_error(&self, connection_id: ConnectionId, error_event: bool) {
        if !error_event || !self.is_current(connection_id) {
            return;
        }

        #[cfg(target_os = "linux")]
        match self.connections[connection_id.slot].socket.take_error() {
            Ok(Some(error)) => eprintln!(
                "connection {} reported a socket error: {error}",
                connection_id.slot
            ),
            Ok(None) => eprintln!(
                "connection {} reported EPOLLERR without SO_ERROR",
                connection_id.slot
            ),
            Err(error) => eprintln!(
                "failed to inspect SO_ERROR for connection {}: {error}",
                connection_id.slot
            ),
        }

        #[cfg(not(target_os = "linux"))]
        eprintln!("connection {} reported a socket error", connection_id.slot);
    }

    fn new(context: WorkerContext) -> io::Result<Self> {
        let WorkerContext {
            listener_groups,
            servers,
            shutdown,
            limits,
        } = context;
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), CONTROL_TOKEN)?);
        shutdown.install_waker(waker);
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
            generations: Vec::new(),
            pending_writes: HashSet::new(),
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

            let scheduled_writes: Vec<ConnectionId> = self.pending_writes.drain().collect();
            for connection_id in scheduled_writes {
                if self.is_current(connection_id) {
                    self.write_ready(connection_id)?;
                }
            }

            let timeout = if self.pending_writes.is_empty() {
                Some(Duration::from_millis(100))
            } else {
                Some(Duration::ZERO)
            };
            match self.poll.poll(&mut events, timeout) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
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
                if token == CONTROL_TOKEN {
                    continue;
                } else if token.0 < self.listeners.len() {
                    if readable && !self.draining {
                        self.accept_ready(token.0)?;
                    }
                } else {
                    let Some(connection_id) = ConnectionId::from_token(token) else {
                        continue;
                    };
                    if !self.is_current(connection_id) {
                        continue;
                    }
                    if error || write_closed {
                        self.log_socket_error(connection_id, error);
                        self.remove_connection(connection_id)?;
                        continue;
                    }
                    if writable {
                        self.write_ready(connection_id)?;
                    }
                    if readable && !self.draining {
                        self.connection_ready(connection_id)?;
                    }
                    if read_closed && self.is_current(connection_id) {
                        let connection = &self.connections[connection_id.slot];
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
                    if let Err(error) = self.register_connection(stream, listener_index) {
                        eprintln!("failed to register connection from {peer_address}: {error}");
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    eprintln!("accept failed on listener {listener_index}: {error}");
                    return Ok(());
                }
            }
        }
    }

    fn register_connection(&mut self, stream: TcpStream, listener_group: usize) -> io::Result<()> {
        let entry = self.connections.vacant_entry();
        let slot = entry.key();
        if slot > SLOT_MASK {
            return Err(io::Error::other("connection slab exceeds token capacity"));
        }
        if self.generations.len() <= slot {
            self.generations.resize(slot + 1, 0);
        }
        let generation = next_generation(self.generations[slot]);
        self.generations[slot] = generation;
        let connection_id = ConnectionId { slot, generation };
        entry.insert(Connection {
            socket: stream,
            read_buffer: Vec::with_capacity(8 * 1024),
            write_buffer: Vec::new(),
            write_offset: 0,
            pending_request: None,
            listener_group,
            last_progress: Instant::now(),
            generation,
        });
        let token = connection_id.token()?;

        if let Err(error) = self.poll.registry().register(
            &mut self.connections[slot].socket,
            token,
            Interest::READABLE,
        ) {
            self.connections.remove(slot);
            return Err(error);
        }

        Ok(())
    }

    fn connection_ready(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }

        let mut close_connection = false;
        let mut error_response = None;
        let mut request: Option<RequestHead> = None;
        let mut listener_group = 0;
        {
            use std::io::Read;

            let connection = &mut self.connections[connection_id.slot];
            let mut buffer = [0_u8; 8 * 1024];

            for _ in 0..MAX_READS_PER_EVENT {
                match connection.socket.read(&mut buffer) {
                    Ok(0) => {
                        close_connection = true;
                        break;
                    }
                    Ok(read) => {
                        if connection.read_buffer.len() + read > self.limits.max_read_buffer_size {
                            error_response = Some((413, "request is too large"));
                            break;
                        }
                        connection.read_buffer.extend_from_slice(&buffer[..read]);
                        connection.last_progress = Instant::now();

                        if connection.pending_request.is_none() {
                            match parse_request_head(&connection.read_buffer) {
                                Ok(RequestHeadParse::Incomplete) => {}
                                Ok(RequestHeadParse::Complete {
                                    request: request_head,
                                    consumed,
                                }) => match request_head.body_length() {
                                    Ok(body_length) => {
                                        let Some(body_end) = consumed.checked_add(body_length)
                                        else {
                                            error_response = Some((413, "request is too large"));
                                            break;
                                        };
                                        if body_end > self.limits.max_read_buffer_size {
                                            error_response = Some((413, "request is too large"));
                                            break;
                                        }
                                        connection.pending_request = Some(PendingRequest {
                                            head: request_head,
                                            body_end,
                                        });
                                    }
                                    Err(BodyFramingError::UnsupportedTransferEncoding) => {
                                        error_response =
                                            Some((501, "transfer encoding is not supported"));
                                        break;
                                    }
                                    Err(
                                        BodyFramingError::InvalidContentLength
                                        | BodyFramingError::ConflictingContentLength,
                                    ) => {
                                        error_response = Some((400, "invalid content length"));
                                        break;
                                    }
                                },
                                Err(error) => {
                                    eprintln!("invalid HTTP request: {error}");
                                    error_response = Some((400, "invalid HTTP request"));
                                    break;
                                }
                            }
                        }

                        if connection
                            .pending_request
                            .as_ref()
                            .is_some_and(|pending| connection.read_buffer.len() >= pending.body_end)
                        {
                            let pending = connection.pending_request.take().unwrap();
                            println!("{} {}", pending.head.method, pending.head.target);
                            listener_group = connection.listener_group;
                            request = Some(pending.head);
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        eprintln!("connection read failed: {error}");
                        close_connection = true;
                        break;
                    }
                }
            }
        }

        if let Some((status, message)) = error_response {
            self.queue_response(connection_id, response_bytes(status, message))?;
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

    fn write_ready(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }

        let mut write_failed = false;
        {
            use std::io::Write;

            let connection = &mut self.connections[connection_id.slot];
            for _ in 0..MAX_WRITES_PER_EVENT {
                if connection.write_offset == connection.write_buffer.len() {
                    break;
                }
                match connection
                    .socket
                    .write(&connection.write_buffer[connection.write_offset..])
                {
                    Ok(0) => {
                        eprintln!("connection write returned zero");
                        write_failed = true;
                        break;
                    }
                    Ok(written) => {
                        connection.write_offset += written;
                        connection.last_progress = Instant::now();
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        eprintln!("connection write failed: {error}");
                        write_failed = true;
                        break;
                    }
                }
            }
        }

        let write_finished = self.is_current(connection_id)
            && self.connections[connection_id.slot].write_offset
                == self.connections[connection_id.slot].write_buffer.len();
        if write_failed || write_finished {
            self.remove_connection(connection_id)?;
        } else if self.is_current(connection_id) {
            // We stopped because the per-event work budget was exhausted, not
            // because the socket returned WouldBlock. Resume it ourselves so
            // edge-triggered epoll does not need to emit another edge.
            self.pending_writes.insert(connection_id);
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
        let close_ids: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(id, connection)| {
                (connection.write_offset >= connection.write_buffer.len()).then_some(ConnectionId {
                    slot: id,
                    generation: connection.generation,
                })
            })
            .collect();
        for connection_id in close_ids {
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn remove_connection(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        if self.is_current(connection_id) {
            self.pending_writes.remove(&connection_id);
            let mut connection = self.connections.remove(connection_id.slot);
            if let Err(error) = self.poll.registry().deregister(&mut connection.socket) {
                eprintln!("failed to deregister connection: {error}");
            }
        }
        Ok(())
    }

    fn queue_response(
        &mut self,
        connection_id: ConnectionId,
        mut response: Vec<u8>,
    ) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }
        if response.len() > self.limits.max_write_buffer_size {
            response = response_bytes(500, "response is too large");
        }

        let reregister = {
            let connection = &mut self.connections[connection_id.slot];
            connection.write_buffer = response;
            connection.write_offset = 0;
            self.poll.registry().reregister(
                &mut connection.socket,
                connection_id.token()?,
                Interest::WRITABLE,
            )
        };
        if let Err(error) = reregister {
            eprintln!("failed to register writable interest: {error}");
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn close_idle_connections(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let expired: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(id, connection)| {
                (now.duration_since(connection.last_progress) >= self.limits.idle_timeout)
                    .then_some(ConnectionId {
                        slot: id,
                        generation: connection.generation,
                    })
            })
            .collect();
        for connection_id in expired {
            eprintln!("connection {} timed out", connection_id.slot);
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn drain_deadline_expired(&self) -> bool {
        self.drain_started_at
            .is_some_and(|started| started.elapsed() >= self.limits.drain_timeout)
    }

    fn close_all_connections(&mut self) {
        let ids: Vec<ConnectionId> = self
            .connections
            .iter()
            .map(|(slot, connection)| ConnectionId {
                slot,
                generation: connection.generation,
            })
            .collect();
        for connection_id in ids {
            if let Err(error) = self.remove_connection(connection_id) {
                eprintln!("failed to close connection during shutdown: {error}");
            }
        }
    }
}

#[cfg(unix)]
fn next_generation(previous: usize) -> usize {
    let next = previous.wrapping_add(1) & GENERATION_MASK;
    if next == 0 {
        1
    } else {
        next
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn connection_tokens_round_trip_slot_and_generation() {
        let id = ConnectionId {
            slot: 42,
            generation: 7,
        };

        assert_eq!(ConnectionId::from_token(id.token().unwrap()), Some(id));
    }

    #[test]
    fn connection_tokens_are_distinct_across_generations() {
        let old = ConnectionId {
            slot: 3,
            generation: 1,
        };
        let replacement = ConnectionId {
            slot: 3,
            generation: next_generation(old.generation),
        };

        assert_ne!(old.token().unwrap(), replacement.token().unwrap());
        assert_eq!(ConnectionId::from_token(old.token().unwrap()), Some(old));
        assert_eq!(
            ConnectionId::from_token(replacement.token().unwrap()),
            Some(replacement)
        );
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
