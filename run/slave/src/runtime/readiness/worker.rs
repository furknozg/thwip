use super::super::{
    DnsLimits, ProxyLimits, ShutdownHandle, WorkerContext, WorkerLimits, WorkerMetrics,
};
#[cfg(unix)]
use crate::proxy::Upstream;
#[cfg(unix)]
use crate::{
    parse_request_head, response_bytes, route, select_server, static_error_response,
    static_stream_response, BodyFramingError, RequestHead, RequestHeadParse, StaticChunk,
    UpstreamBalancer,
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
use super::connection::{Connection, PendingRequest};
#[cfg(unix)]
use super::proxy::{ProxyPhase, ProxyState};
#[cfg(unix)]
use super::resolver::{DnsResolver, ResolveResult};
#[cfg(unix)]
use super::token::{next_generation, ConnectionId, SocketRole, CONTROL_TOKEN, SLOT_MASK};

#[cfg(unix)]
const MAX_READS_PER_EVENT: usize = 16;
#[cfg(unix)]
const MAX_WRITES_PER_EVENT: usize = 16;

#[cfg(unix)]
struct ReadinessWorker {
    poll: Poll,
    listeners: Vec<RegisteredListener>,
    connections: Slab<Connection>,
    generations: Vec<usize>,
    pending_writes: HashSet<ConnectionId>,
    pending_upstream_writes: HashSet<ConnectionId>,
    pending_upstream_reads: HashSet<ConnectionId>,
    servers: Vec<Server>,
    shutdown: ShutdownHandle,
    limits: WorkerLimits,
    proxy_limits: ProxyLimits,
    dns_limits: DnsLimits,
    resolver: DnsResolver,
    metrics: WorkerMetrics,
    balancer: UpstreamBalancer,
    draining: bool,
    drain_started_at: Option<Instant>,
}

#[cfg(unix)]
struct RegisteredListener {
    socket: TcpListener,
    default_server: usize,
    server_indices: Vec<usize>,
}

/// Snapshot the readiness flags before mutating worker state. `mio::Events`
/// borrows the poll buffer, while dispatch may register or remove sockets.
#[derive(Clone, Copy)]
struct ReadyEvent {
    token: Token,
    readable: bool,
    writable: bool,
    error: bool,
    read_closed: bool,
    write_closed: bool,
}

#[cfg(unix)]
impl ReadinessWorker {
    fn is_current(&self, connection_id: ConnectionId) -> bool {
        self.connections
            .get(connection_id.slot)
            .is_some_and(|connection| connection.generation == connection_id.generation)
    }

    fn log_socket_error(&self, connection_id: ConnectionId, error_event: bool) {
        if !error_event || !self.is_current(connection_id) {
            return;
        }
        self.metrics.error();

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
            proxy_limits,
            dns_limits,
            metrics,
        } = context;
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), CONTROL_TOKEN)?);
        shutdown.install_waker(Arc::clone(&waker));
        let resolver = DnsResolver::new(dns_limits.resolver_threads, waker)?;
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
            pending_upstream_writes: HashSet::new(),
            pending_upstream_reads: HashSet::new(),
            servers,
            shutdown,
            limits,
            proxy_limits,
            dns_limits,
            resolver,
            metrics,
            balancer: UpstreamBalancer::default(),
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
            self.process_dns_results()?;
            self.close_expired_resolutions()?;
            self.close_expired_proxies()?;
            self.close_idle_connections()?;
            self.pump_static_streams()?;

            let scheduled_writes: Vec<ConnectionId> = self.pending_writes.drain().collect();
            for connection_id in scheduled_writes {
                if self.is_current(connection_id) {
                    self.write_ready(connection_id)?;
                }
            }
            let upstream_writes: Vec<ConnectionId> = self.pending_upstream_writes.drain().collect();
            for connection_id in upstream_writes {
                if self.is_current(connection_id) {
                    self.write_upstream_request(connection_id)?;
                }
            }
            let upstream_reads: Vec<ConnectionId> = self.pending_upstream_reads.drain().collect();
            for connection_id in upstream_reads {
                if self.is_current(connection_id) {
                    self.read_upstream_response(connection_id)?;
                }
            }

            let timeout = if self.pending_writes.is_empty()
                && self.pending_upstream_writes.is_empty()
                && self.pending_upstream_reads.is_empty()
            {
                Some(Duration::from_millis(100))
            } else {
                Some(Duration::ZERO)
            };
            match self.poll.poll(&mut events, timeout) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
            let ready: Vec<ReadyEvent> = events
                .iter()
                .map(|event| ReadyEvent {
                    token: event.token(),
                    readable: event.is_readable(),
                    writable: event.is_writable(),
                    error: event.is_error(),
                    read_closed: event.is_read_closed(),
                    write_closed: event.is_write_closed(),
                })
                .collect();

            for event in ready {
                self.dispatch(event)?;
            }
        }
    }

    /// Decode one token and hand the readiness flags to the owner of that
    /// socket. This is the only place that maps OS events to application state.
    fn dispatch(&mut self, event: ReadyEvent) -> io::Result<()> {
        if event.token == CONTROL_TOKEN {
            return self.process_dns_results();
        }
        if event.token.0 < self.listeners.len() {
            if event.readable && !self.draining {
                self.accept_ready(event.token.0)?;
            }
            return Ok(());
        }

        let Some((connection_id, role)) = ConnectionId::from_token(event.token) else {
            return Ok(());
        };
        if !self.is_current(connection_id) {
            return Ok(());
        }
        match role {
            SocketRole::Client => self.dispatch_client(connection_id, event),
            SocketRole::Upstream => self.dispatch_upstream(connection_id, event),
        }
    }

    fn dispatch_client(&mut self, id: ConnectionId, event: ReadyEvent) -> io::Result<()> {
        if event.error {
            self.log_socket_error(id, true);
            return self.remove_connection(id);
        }
        if event.writable {
            self.write_ready(id)?;
        }
        if event.readable && !self.draining {
            self.connection_ready(id)?;
        }
        if (event.read_closed || event.write_closed) && self.is_current(id) {
            let connection = &self.connections[id.slot];
            if connection.write_offset == connection.write_buffer.len() {
                self.remove_connection(id)?;
            }
        }
        Ok(())
    }

    fn dispatch_upstream(&mut self, id: ConnectionId, event: ReadyEvent) -> io::Result<()> {
        if !self.connections[id.slot].is_proxying() {
            return Ok(());
        }
        if event.error {
            return self.fail_proxy(id, 502, "upstream connection failed");
        }
        if event.writable {
            self.write_upstream_request(id)?;
        }
        if event.readable && self.is_current(id) {
            self.read_upstream_response(id)?;
        }
        if (event.read_closed || event.write_closed) && self.is_current(id) {
            // Close notifications can accompany unread response bytes. recv(0)
            // remains the single transition to final upstream EOF.
            self.pending_upstream_reads.insert(id);
        }
        Ok(())
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
                    self.metrics.accepted();
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
        entry.insert(Connection::new(stream, listener_group, generation));
        let token = connection_id.token(SocketRole::Client)?;

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
        if self.connections[connection_id.slot].is_handling_request() {
            return self.client_during_proxy_ready(connection_id);
        }

        let mut close_connection = false;
        let mut error_response = None;
        let mut request: Option<RequestHead> = None;
        let mut request_body = Vec::new();
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
                        self.metrics.read_bytes(read);
                        if connection.read_buffer.len() + read > self.limits.max_read_buffer_size {
                            error_response = Some((413, "request is too large"));
                            break;
                        }
                        connection.read_buffer.extend_from_slice(&buffer[..read]);
                        connection.last_progress = Instant::now();

                        if connection.pending_request().is_none() {
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
                                        connection.set_pending_request(PendingRequest {
                                            head: request_head,
                                            body_start: consumed,
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
                            .pending_request()
                            .as_ref()
                            .is_some_and(|pending| connection.read_buffer.len() >= pending.body_end)
                        {
                            let pending = connection.take_pending_request().unwrap();
                            println!("{} {}", pending.head.method, pending.head.target);
                            listener_group = connection.listener_group;
                            request_body = connection.read_buffer
                                [pending.body_start..pending.body_end]
                                .to_vec();
                            request = Some(pending.head);
                            self.metrics.request();
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
            let Some(server) = self.servers.get(server_index) else {
                self.queue_response(
                    connection_id,
                    response_bytes(500, "server configuration is unavailable"),
                )?;
                return Ok(());
            };
            let action = route(server, &request.target).cloned();
            match action {
                Some(action @ Action::Proxy { .. }) => {
                    let upstream = match self.balancer.select(&action) {
                        Ok(upstream) => upstream,
                        Err(error) => {
                            eprintln!("invalid upstream group: {error}");
                            self.queue_response(
                                connection_id,
                                response_bytes(502, "upstream configuration failed"),
                            )?;
                            return Ok(());
                        }
                    };
                    if let Err(error) =
                        self.start_proxy(connection_id, &upstream, &request, &request_body)
                    {
                        eprintln!("failed to start upstream proxy: {error}");
                        self.queue_response(
                            connection_id,
                            response_bytes(502, "upstream connection failed"),
                        )?;
                    }
                }
                action => match action.as_ref() {
                    Some(Action::Static { directory }) => {
                        match static_stream_response(directory.as_ref(), &request) {
                            Ok(response) => self.queue_static_response(connection_id, response)?,
                            Err(error) => {
                                self.queue_response(connection_id, static_error_response(error))?
                            }
                        }
                    }
                    _ => {
                        let response = self.response_for(server_index, &request.target);
                        self.queue_response(connection_id, response)?;
                    }
                },
            }
        }

        if close_connection {
            self.remove_connection(connection_id)?;
        }

        Ok(())
    }

    fn client_during_proxy_ready(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        use std::io::Read;

        let mut close = false;
        let connection = &mut self.connections[connection_id.slot];
        let mut buffer = [0_u8; 8 * 1024];
        for _ in 0..MAX_READS_PER_EVENT {
            match connection.socket.read(&mut buffer) {
                Ok(0) => {
                    close = true;
                    break;
                }
                Ok(_) => connection.last_progress = Instant::now(),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    eprintln!("proxy client read failed: {error}");
                    close = true;
                    break;
                }
            }
        }
        if close {
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn start_proxy(
        &mut self,
        connection_id: ConnectionId,
        upstream_url: &str,
        request: &RequestHead,
        body: &[u8],
    ) -> io::Result<()> {
        let upstream = Upstream::parse(upstream_url)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let request_buffer = upstream.request_bytes(request, body);
        if request_buffer.len() > self.limits.max_read_buffer_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "upstream request exceeds the configured read-buffer limit",
            ));
        }

        let connect_address = upstream.connect_address().to_owned();
        self.connections[connection_id.slot].begin_resolving(request_buffer);
        self.resolver.resolve(connection_id, connect_address)
    }

    fn process_dns_results(&mut self) -> io::Result<()> {
        for result in self.resolver.drain() {
            self.finish_resolution(result)?;
        }
        Ok(())
    }

    fn finish_resolution(&mut self, result: ResolveResult) -> io::Result<()> {
        let connection_id = result.connection_id;
        if !self.is_current(connection_id) || !self.connections[connection_id.slot].is_resolving() {
            return Ok(());
        }

        let addresses = match result.addresses {
            Ok(addresses) if !addresses.is_empty() => addresses,
            Ok(_) => {
                return self.fail_resolution(connection_id, 502, "upstream has no address");
            }
            Err(error) => {
                eprintln!("upstream DNS resolution failed: {error}");
                return self.fail_resolution(connection_id, 502, "upstream DNS resolution failed");
            }
        };
        let Some(resolution) = self.connections[connection_id.slot].take_resolution() else {
            return Ok(());
        };
        let mut socket = match TcpStream::connect(addresses[0]) {
            Ok(socket) => socket,
            Err(error) => {
                eprintln!("failed to connect resolved upstream: {error}");
                return self.queue_response(
                    connection_id,
                    response_bytes(502, "upstream connection failed"),
                );
            }
        };
        if let Err(error) = self.poll.registry().register(
            &mut socket,
            connection_id.token(SocketRole::Upstream)?,
            Interest::READABLE.add(Interest::WRITABLE),
        ) {
            eprintln!("failed to register resolved upstream: {error}");
            return self.queue_response(
                connection_id,
                response_bytes(502, "upstream connection failed"),
            );
        }
        self.connections[connection_id.slot]
            .begin_proxy(ProxyState::new(socket, resolution.request_buffer));
        Ok(())
    }

    fn fail_resolution(
        &mut self,
        connection_id: ConnectionId,
        status: u16,
        message: &str,
    ) -> io::Result<()> {
        if !self.is_current(connection_id) || !self.connections[connection_id.slot].is_resolving() {
            return Ok(());
        }
        self.queue_response(connection_id, response_bytes(status, message))
    }

    fn write_upstream_request(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        use std::io::Write;

        if !self.is_current(connection_id) || !self.connections[connection_id.slot].is_proxying() {
            return Ok(());
        }
        if self.connections[connection_id.slot]
            .proxy()
            .is_some_and(|proxy| proxy.phase == ProxyPhase::ReadingResponse)
        {
            return Ok(());
        }

        let mut failed = None;
        let mut finished = false;
        let mut made_progress = false;
        {
            let connection = &mut self.connections[connection_id.slot];
            let proxy = connection.proxy_mut().unwrap();
            if proxy.phase == ProxyPhase::Connecting {
                if let Some(error) = proxy.upstream.take_error()? {
                    failed = Some(error);
                } else {
                    proxy.transition(ProxyPhase::WritingRequest);
                }
            }
            if failed.is_none() && proxy.phase == ProxyPhase::WritingRequest {
                for _ in 0..MAX_WRITES_PER_EVENT {
                    if proxy.request_offset == proxy.request_buffer.len() {
                        finished = true;
                        break;
                    }
                    match proxy
                        .upstream
                        .write(&proxy.request_buffer[proxy.request_offset..])
                    {
                        Ok(0) => {
                            failed = Some(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "upstream write returned zero",
                            ));
                            break;
                        }
                        Ok(written) => {
                            proxy.request_offset += written;
                            proxy.record_progress();
                            made_progress = true;
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => {
                            failed = Some(error);
                            break;
                        }
                    }
                }
                finished |= proxy.request_offset == proxy.request_buffer.len();
            }
        }

        if made_progress && self.is_current(connection_id) {
            self.connections[connection_id.slot].last_progress = Instant::now();
        }

        if let Some(error) = failed {
            eprintln!("upstream request write failed: {error}");
            return self.fail_proxy(connection_id, 502, "upstream connection failed");
        }
        if finished {
            let reregister = {
                let proxy = self.connections[connection_id.slot].proxy_mut().unwrap();
                proxy.transition(ProxyPhase::ReadingResponse);
                self.poll.registry().reregister(
                    &mut proxy.upstream,
                    connection_id.token(SocketRole::Upstream)?,
                    Interest::READABLE,
                )
            };
            if let Err(error) = reregister {
                eprintln!("failed to register upstream readable interest: {error}");
                self.fail_proxy(connection_id, 502, "upstream connection failed")?;
            }
        } else {
            self.pending_upstream_writes.insert(connection_id);
        }
        Ok(())
    }

    fn read_upstream_response(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        use std::io::Read;

        if !self.is_current(connection_id)
            || !self.connections[connection_id.slot].is_proxying()
            || !self.connections[connection_id.slot].write_buffer.is_empty()
            || self.connections[connection_id.slot]
                .proxy()
                .is_some_and(|proxy| proxy.phase != ProxyPhase::ReadingResponse)
        {
            return Ok(());
        }

        let mut buffer = [0_u8; 8 * 1024];
        let read_limit = buffer.len().min(self.limits.max_write_buffer_size);
        let result = {
            let connection = &mut self.connections[connection_id.slot];
            let proxy = connection.proxy_mut().unwrap();
            loop {
                match proxy.upstream.read(&mut buffer[..read_limit]) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result,
                }
            }
        };

        match result {
            Ok(0) => self.mark_upstream_eof(connection_id),
            Ok(read) => {
                self.metrics.read_bytes(read);
                let connection = &mut self.connections[connection_id.slot];
                {
                    let proxy = connection.proxy_mut().unwrap();
                    proxy.response_started = true;
                    proxy.record_progress();
                }
                connection.write_buffer.extend_from_slice(&buffer[..read]);
                connection.write_offset = 0;
                connection.last_progress = Instant::now();
                self.reregister_client(connection_id, Interest::WRITABLE)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => {
                eprintln!("upstream response read failed: {error}");
                self.fail_proxy(connection_id, 502, "upstream response failed")
            }
        }
    }

    fn mark_upstream_eof(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }
        if let Some(proxy) = self.connections[connection_id.slot].proxy_mut() {
            proxy.upstream_eof = true;
            if let Err(error) = self.poll.registry().deregister(&mut proxy.upstream) {
                eprintln!("failed to deregister completed upstream: {error}");
            }
        }
        if self.connections[connection_id.slot].write_buffer.is_empty() {
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn fail_proxy(
        &mut self,
        connection_id: ConnectionId,
        status: u16,
        message: &str,
    ) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }
        eprintln!("proxy connection {} failed: {message}", connection_id.slot);
        let can_send_error = self.connections[connection_id.slot]
            .proxy()
            .is_some_and(|proxy| !proxy.response_started)
            && self.connections[connection_id.slot].write_buffer.is_empty();
        if let Some(mut proxy) = self.connections[connection_id.slot].detach_proxy() {
            #[cfg(target_os = "linux")]
            if let Ok(Some(error)) = proxy.upstream.take_error() {
                eprintln!("upstream SO_ERROR: {error}");
            }
            if let Err(error) = self.poll.registry().deregister(&mut proxy.upstream) {
                eprintln!("failed to deregister failed upstream: {error}");
            }
        }
        self.pending_upstream_reads.remove(&connection_id);
        self.pending_upstream_writes.remove(&connection_id);
        if can_send_error {
            self.queue_response(connection_id, response_bytes(status, message))
        } else {
            self.remove_connection(connection_id)
        }
    }

    fn reregister_client(
        &mut self,
        connection_id: ConnectionId,
        interest: Interest,
    ) -> io::Result<()> {
        let result = {
            let connection = &mut self.connections[connection_id.slot];
            self.poll.registry().reregister(
                &mut connection.socket,
                connection_id.token(SocketRole::Client)?,
                interest,
            )
        };
        if let Err(error) = result {
            eprintln!("failed to reregister proxy client: {error}");
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
                        self.metrics.wrote_bytes(written);
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
        if write_failed {
            self.remove_connection(connection_id)?;
        } else if write_finished && self.is_current(connection_id) {
            if self.connections[connection_id.slot].static_stream.is_some() {
                return self.advance_static_stream(connection_id);
            }
            self.metrics.response();
            let proxy_state = self.connections[connection_id.slot]
                .proxy()
                .map(|proxy| proxy.upstream_eof);
            match proxy_state {
                None => self.remove_connection(connection_id)?,
                Some(_) if self.draining => self.remove_connection(connection_id)?,
                Some(true) => self.remove_connection(connection_id)?,
                Some(false) => {
                    let connection = &mut self.connections[connection_id.slot];
                    connection.write_buffer.clear();
                    connection.write_offset = 0;
                    self.reregister_client(connection_id, Interest::READABLE)?;
                    self.pending_upstream_reads.insert(connection_id);
                }
            }
        } else if self.is_current(connection_id) {
            // We stopped because the per-event work budget was exhausted, not
            // because the socket returned WouldBlock. Resume it ourselves so
            // edge-triggered epoll does not need to emit another edge.
            self.pending_writes.insert(connection_id);
        }

        Ok(())
    }

    fn response_for(&self, server_index: usize, target: &str) -> Vec<u8> {
        let Some(server) = self.servers.get(server_index) else {
            return response_bytes(500, "server configuration is unavailable");
        };

        match route(server, target) {
            Some(Action::Response { status, body }) => response_bytes(*status, body),
            Some(Action::Static { .. }) => response_bytes(500, "static stream was not prepared"),
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
                (connection.static_stream.is_none()
                    && connection.write_offset >= connection.write_buffer.len())
                .then_some(ConnectionId {
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
            self.pending_upstream_reads.remove(&connection_id);
            self.pending_upstream_writes.remove(&connection_id);
            let mut connection = self.connections.remove(connection_id.slot);
            if let Some(mut proxy) = connection.detach_proxy() {
                if !proxy.upstream_eof {
                    if let Err(error) = self.poll.registry().deregister(&mut proxy.upstream) {
                        eprintln!("failed to deregister upstream connection: {error}");
                    }
                }
            }
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
            connection.begin_response();
            connection.write_buffer = response;
            connection.write_offset = 0;
            self.poll.registry().reregister(
                &mut connection.socket,
                connection_id.token(SocketRole::Client)?,
                Interest::WRITABLE,
            )
        };
        if let Err(error) = reregister {
            eprintln!("failed to register writable interest: {error}");
            self.remove_connection(connection_id)?;
        }
        Ok(())
    }

    fn queue_static_response(
        &mut self,
        connection_id: ConnectionId,
        response: crate::static_files::StaticStreamResponse,
    ) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }
        self.connections[connection_id.slot].static_stream = response.stream;
        self.queue_response(connection_id, response.head)
    }

    fn pump_static_streams(&mut self) -> io::Result<()> {
        let ready: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(slot, connection)| {
                (connection.static_stream.is_some()
                    && connection.write_offset == connection.write_buffer.len())
                .then_some(ConnectionId {
                    slot,
                    generation: connection.generation,
                })
            })
            .collect();
        for connection_id in ready {
            self.advance_static_stream(connection_id)?;
        }
        Ok(())
    }

    fn advance_static_stream(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        if !self.is_current(connection_id) {
            return Ok(());
        }
        let next = self.connections[connection_id.slot]
            .static_stream
            .as_ref()
            .unwrap()
            .try_next();
        match next {
            Ok(StaticChunk::Data(bytes)) => {
                let connection = &mut self.connections[connection_id.slot];
                connection.write_buffer = bytes;
                connection.write_offset = 0;
                self.poll.registry().reregister(
                    &mut connection.socket,
                    connection_id.token(SocketRole::Client)?,
                    Interest::WRITABLE,
                )?;
            }
            Ok(StaticChunk::Pending) => {
                let connection = &mut self.connections[connection_id.slot];
                self.poll.registry().reregister(
                    &mut connection.socket,
                    connection_id.token(SocketRole::Client)?,
                    Interest::READABLE,
                )?;
            }
            Ok(StaticChunk::Finished) => {
                self.metrics.response();
                self.remove_connection(connection_id)?;
            }
            Err(error) => {
                self.metrics.error();
                eprintln!("static file stream failed: {error}");
                self.remove_connection(connection_id)?;
            }
        }
        Ok(())
    }

    fn close_idle_connections(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let expired: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(id, connection)| {
                (!connection.is_handling_request()
                    && now.duration_since(connection.last_progress) >= self.limits.idle_timeout)
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

    fn close_expired_resolutions(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let expired: Vec<ConnectionId> =
            self.connections
                .iter()
                .filter_map(|(slot, connection)| {
                    let resolution = connection.resolution()?;
                    (now.duration_since(resolution.started_at) >= self.dns_limits.timeout)
                        .then_some(ConnectionId {
                            slot,
                            generation: connection.generation,
                        })
                })
                .collect();

        for connection_id in expired {
            self.fail_resolution(connection_id, 504, "upstream DNS resolution timed out")?;
        }
        Ok(())
    }

    /// Proxy timeouts are phase-specific: connecting, sending the request, and
    /// waiting for response progress each have independent policies.
    fn close_expired_proxies(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let expired: Vec<(ConnectionId, ProxyPhase)> = self
            .connections
            .iter()
            .filter_map(|(slot, connection)| {
                let proxy = connection.proxy()?;
                let timeout = proxy.phase.timeout(self.proxy_limits);
                (now.duration_since(proxy.phase_progress_at) >= timeout).then_some((
                    ConnectionId {
                        slot,
                        generation: connection.generation,
                    },
                    proxy.phase,
                ))
            })
            .collect();

        for (connection_id, phase) in expired {
            self.fail_proxy(connection_id, 504, phase.timeout_message())?;
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
pub(crate) fn run(context: WorkerContext, max_events: usize) -> io::Result<()> {
    ReadinessWorker::new(context)?.run(max_events)
}
