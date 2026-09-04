use super::{
    buffer_ring::{ProvidedBufferRing, BUFFER_GROUP},
    capabilities::Capabilities,
    connection::{
        next_generation, CompletedRequest, ConnectionId, PendingRequest, ProxyPhase,
        UringConnection, UringProxy,
    },
    listener::{AcceptMode, UringListener},
    operation::{OperationId, OperationKind},
    resolver::{DnsResolver, ResolveResult},
    IoUringRuntime,
};
use crate::proxy::Upstream;
use crate::{
    parse_request_head, response_bytes, route, select_server, static_error_response,
    static_stream_response, BodyFramingError, DnsLimits, ProxyLimits, RequestHead,
    RequestHeadParse, ShutdownHandle, StaticChunk, UpstreamBalancer, WorkerContext, WorkerLimits,
    WorkerMetrics,
};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use proxy_common::Action;
use proxy_common::Server;
use slab::Slab;
use socket2::SockAddr;
use std::{
    io,
    io::{Cursor, Read, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::Arc,
};

pub struct IoUringWorker {
    ring: IoUring<squeue::Entry, cqueue::Entry>,
    // Declared after `ring` so the kernel ring is dropped before this memory.
    buffer_ring: ProvidedBufferRing,
    listeners: Vec<UringListener>,
    servers: Vec<Server>,
    shutdown: ShutdownHandle,
    connections: Slab<UringConnection>,
    generations: Vec<u16>,
    buffer_size: usize,
    limits: WorkerLimits,
    proxy_limits: ProxyLimits,
    dns_limits: DnsLimits,
    shutdown_eventfd: Arc<OwnedFd>,
    shutdown_value: Box<u64>,
    shutting_down: bool,
    shutdown_started: Option<std::time::Instant>,
    cancellation_started: bool,
    pending_cancellations: usize,
    dns_eventfd: Arc<OwnedFd>,
    dns_value: Box<u64>,
    resolver: DnsResolver,
    timerfd: OwnedFd,
    timer_value: Box<u64>,
    metrics: WorkerMetrics,
    balancer: UpstreamBalancer,
}

struct Completion {
    operation: OperationId,
    result: i32,
    flags: u32,
    buffer_id: Option<u16>,
}

impl IoUringWorker {
    /// Build the ring and take ownership of every listener in the worker context.
    pub(super) fn new(runtime: IoUringRuntime, context: WorkerContext) -> io::Result<Self> {
        let buffer_size = runtime.buf_size;
        let ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
            .setup_cqsize(runtime.cq_entries)
            .build(runtime.sq_entries)?;
        let capabilities = Capabilities::probe(&ring)?;
        capabilities.validate_required()?;
        let buffer_ring =
            ProvidedBufferRing::register(&ring, runtime.buf_ring_size, runtime.buf_size)?;

        if context.ssl_configs.len() != context.servers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSL configuration does not match the configured servers",
            ));
        }
        let listeners = context
            .listener_groups
            .into_iter()
            .map(|group| {
                let ssl = context
                    .ssl_configs
                    .get(group.default_server)
                    .cloned()
                    .flatten();
                UringListener::new(
                    group.socket.into(),
                    group.default_server,
                    group.server_indices,
                    ssl,
                    AcceptMode::Multishot,
                )
            })
            .collect();
        let shutdown_eventfd = Arc::new(create_eventfd()?);
        context
            .shutdown
            .install_eventfd(Arc::clone(&shutdown_eventfd));
        let dns_eventfd = Arc::new(create_eventfd()?);
        let resolver = DnsResolver::new(
            context.dns_limits.resolver_threads,
            Arc::clone(&dns_eventfd),
        )?;
        let timerfd = create_timerfd()?;

        Ok(Self {
            ring,
            buffer_ring,
            listeners,
            shutdown: context.shutdown,
            connections: Slab::new(),
            generations: Vec::new(),
            buffer_size,
            servers: context.servers,
            limits: context.limits,
            proxy_limits: context.proxy_limits,
            dns_limits: context.dns_limits,
            shutdown_eventfd,
            shutdown_value: Box::new(0),
            shutting_down: false,
            shutdown_started: None,
            cancellation_started: false,
            pending_cancellations: 0,
            dns_eventfd,
            dns_value: Box::new(0),
            resolver,
            timerfd,
            timer_value: Box::new(0),
            metrics: context.metrics,
            balancer: UpstreamBalancer::with_groups(context.upstream_groups),
        })
    }

    pub fn submit_accept(&mut self, listener_idx: usize) -> io::Result<()> {
        let listener = self.listeners.get(listener_idx).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "listener index is unavailable")
        })?;
        if listener.accept_pending() {
            return Ok(());
        }
        let entry = listener.accept_entry(listener_idx)?;

        push_entry(&mut self.ring, &entry)?;

        self.listeners[listener_idx].mark_accept_submitted();
        Ok(())
    }

    pub fn submit_initial_accepts(&mut self) -> io::Result<()> {
        for listener_idx in 0..self.listeners.len() {
            self.submit_accept(listener_idx)?;
        }

        self.ring.submit()?;
        Ok(())
    }

    fn dispatch_completion(&mut self, completion: Completion) -> io::Result<()> {
        match completion.operation.kind {
            OperationKind::Accept => self.handle_accept_completion(
                completion.operation,
                completion.result,
                completion.flags,
            ),
            OperationKind::Read => self.handle_read_completion(
                completion.operation,
                completion.result,
                completion.buffer_id,
            ),
            OperationKind::Write => {
                self.handle_write_completion(completion.operation, completion.result)
            }
            OperationKind::ProxyConnect => {
                self.handle_proxy_connect(completion.operation, completion.result)
            }
            OperationKind::ProxyWrite => {
                self.handle_proxy_write(completion.operation, completion.result)
            }
            OperationKind::ProxyRead => self.handle_proxy_read(
                completion.operation,
                completion.result,
                completion.buffer_id,
            ),
        }
    }

    fn handle_accept_completion(
        &mut self,
        operation: OperationId,
        result: i32,
        flags: u32,
    ) -> io::Result<()> {
        // A successful CQE transfers ownership of a new descriptor even if its
        // operation ID is stale, so wrap it before any early return.
        let accepted = (result >= 0).then(|| unsafe { OwnedFd::from_raw_fd(result) });
        let listener_idx = operation.slot as usize;
        let Some(listener) = self.listeners.get_mut(listener_idx) else {
            return Ok(());
        };
        if !listener.matches_generation(operation.generation) {
            return Ok(());
        }
        let accept_mode = listener.accept_mode();
        listener.record_completion(cqueue::more(flags));

        if accept_mode == AcceptMode::Multishot && multishot_is_unsupported(result) {
            listener.fall_back_to_single_shot();
            eprintln!(
                "listener {listener_idx}: multishot accept is unavailable; falling back to single-shot"
            );
        }

        if let Some(accepted) = accepted {
            if !self.shutting_down {
                self.metrics.accepted();
                match self.insert_connection(accepted, listener_idx) {
                    Ok(connection_id) => self.submit_recv(connection_id)?,
                    Err(error) => eprintln!("failed to retain accepted connection: {error}"),
                }
            }
        } else if !(self.shutting_down && -result == libc::ECANCELED)
            && !multishot_is_unsupported(result)
        {
            self.metrics.error();
            let error = io::Error::from_raw_os_error(-result);
            eprintln!("listener {listener_idx} accept failed: {error}");
        }

        if !self.shutdown.is_requested() && !self.listeners[listener_idx].accept_pending() {
            self.submit_accept(listener_idx)?;
        }
        Ok(())
    }

    fn submit_send(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let connection = self
            .connections
            .get_mut(connection_id.slot as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connection slot is unavailable",
                )
            })?;
        if !connection.matches_generation(connection_id.generation) || connection.write_pending {
            return Ok(());
        }
        let remaining = if let Some(ssl) = &mut connection.ssl {
            if connection.ssl_write_offset == connection.ssl_write_buffer.len() {
                connection.ssl_write_buffer.clear();
                connection.ssl_write_offset = 0;
                if connection.write_offset < connection.write_buffer.len() {
                    let written = ssl
                        .connection
                        .writer()
                        .write(&connection.write_buffer[connection.write_offset..])?;
                    connection.write_offset += written;
                }
                if ssl.connection.wants_write() {
                    ssl.connection.write_tls(&mut connection.ssl_write_buffer)?;
                }
            }
            &connection.ssl_write_buffer[connection.ssl_write_offset..]
        } else {
            &connection.write_buffer[connection.write_offset..]
        };
        if remaining.is_empty() {
            return Ok(());
        }
        let write_len = u32::try_from(remaining.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "response exceeds io_uring send length capacity",
            )
        })?;
        let entry = opcode::Send::new(
            types::Fd(connection.socket.as_raw_fd()),
            remaining.as_ptr(),
            write_len,
        )
        .flags(libc::MSG_NOSIGNAL)
        .build()
        .user_data(OperationId::write(connection_id.slot, connection_id.generation).encode());

        push_entry(&mut self.ring, &entry)?;
        connection.mark_write_submitted();
        Ok(())
    }

    fn handle_write_completion(&mut self, operation: OperationId, result: i32) -> io::Result<()> {
        let slot = operation.slot as usize;
        let Some(connection) = self.connections.get_mut(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(operation.generation) {
            return Ok(());
        }
        connection.mark_write_completed();
        if result < 0 {
            let error = io::Error::from_raw_os_error(-result);
            if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
                return self.submit_send(ConnectionId {
                    slot: operation.slot,
                    generation: operation.generation,
                });
            }
            eprintln!("connection {slot} send failed: {error}");
            self.connections.remove(slot);
            return Ok(());
        }
        if result == 0 {
            self.connections.remove(slot);
            return Ok(());
        }

        let written = result as usize;
        self.metrics.wrote_bytes(written);
        let remaining = if connection.ssl.is_some() {
            connection.ssl_write_buffer.len() - connection.ssl_write_offset
        } else {
            connection.write_buffer.len() - connection.write_offset
        };
        if written > remaining {
            eprintln!("connection {slot} returned an invalid send length");
            self.connections.remove(slot);
            return Ok(());
        }
        if connection.ssl.is_some() {
            connection.ssl_write_offset += written;
        } else {
            connection.write_offset += written;
        }
        let output_drained = connection.ssl.as_ref().map_or(
            connection.write_offset == connection.write_buffer.len(),
            |ssl| {
                connection.ssl_write_offset == connection.ssl_write_buffer.len()
                    && connection.write_offset == connection.write_buffer.len()
                    && !ssl.connection.wants_write()
            },
        );
        if output_drained {
            if connection.static_stream.is_some() {
                connection.write_buffer.clear();
                connection.write_offset = 0;
                return self.advance_static_stream(ConnectionId {
                    slot: operation.slot,
                    generation: operation.generation,
                });
            }
            if let Some(proxy) = connection.proxy.as_mut() {
                connection.write_buffer.clear();
                connection.write_offset = 0;
                proxy.record_progress();
                return self.submit_proxy_read(ConnectionId {
                    slot: operation.slot,
                    generation: operation.generation,
                });
            }
            if let Some(ssl) = connection.ssl.as_mut() {
                if !ssl.closing {
                    ssl.connection.send_close_notify();
                    ssl.closing = true;
                    return self.submit_send(ConnectionId {
                        slot: operation.slot,
                        generation: operation.generation,
                    });
                }
            }
            self.metrics.response();
            self.connections.remove(slot);
            return Ok(());
        }

        self.submit_send(ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        })
    }

    fn insert_connection(
        &mut self,
        socket: OwnedFd,
        listener_index: usize,
    ) -> io::Result<ConnectionId> {
        if self.connections.len() >= self.limits.max_connections {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "connection limit reached",
            ));
        }

        let entry = self.connections.vacant_entry();
        let slot = entry.key();
        let operation_slot = u32::try_from(slot).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection index exceeds io_uring operation capacity",
            )
        })?;
        if self.generations.len() <= slot {
            self.generations.resize(slot + 1, 0);
        }
        let generation = next_generation(self.generations[slot]);
        self.generations[slot] = generation;
        let ssl = self.listeners[listener_index].ssl.as_ref();
        let connection = entry.insert(UringConnection::new(
            socket,
            generation,
            listener_index,
            self.buffer_size,
            ssl,
        )?);

        Ok(connection.id(operation_slot))
    }

    fn submit_recv(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let connection = self
            .connections
            .get_mut(connection_id.slot as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connection slot is unavailable",
                )
            })?;
        if !connection.matches_generation(connection_id.generation) || connection.read_pending {
            return Ok(());
        }
        let read_len = u32::try_from(self.buffer_size).unwrap();
        let entry = opcode::Recv::new(
            types::Fd(connection.socket.as_raw_fd()),
            std::ptr::null_mut(),
            read_len,
        )
        .buf_group(BUFFER_GROUP)
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(OperationId::read(connection_id.slot, connection_id.generation).encode());

        push_entry(&mut self.ring, &entry)?;
        connection.mark_read_submitted();
        Ok(())
    }

    fn handle_read_completion(
        &mut self,
        operation: OperationId,
        result: i32,
        buffer_id: Option<u16>,
    ) -> io::Result<()> {
        let received = if result > 0 {
            let id = buffer_id.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "receive completed without a selected buffer",
                )
            })?;
            Some(self.buffer_ring.copy_and_release(id, result as usize)?)
        } else {
            if let Some(id) = buffer_id {
                self.buffer_ring.copy_and_release(id, 0)?;
            }
            None
        };
        let slot = operation.slot as usize;
        let Some(connection) = self.connections.get_mut(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(operation.generation) {
            return Ok(());
        }
        connection.mark_read_completed();

        if result <= 0 {
            if result < 0 {
                let error = io::Error::from_raw_os_error(-result);
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EAGAIN | libc::EINTR | libc::ENOBUFS)
                ) {
                    return self.submit_recv(ConnectionId {
                        slot: operation.slot,
                        generation: operation.generation,
                    });
                }
                eprintln!("connection {slot} receive failed: {error}");
            }
            self.connections.remove(slot);
            return Ok(());
        }

        let received = received.unwrap();
        let received = if let Some(ssl) = &mut connection.ssl {
            let mut encrypted = Cursor::new(received);
            if let Err(error) = ssl.connection.read_tls(&mut encrypted) {
                eprintln!("connection {slot} SSL receive failed: {error}");
                self.connections.remove(slot);
                return Ok(());
            }
            if let Err(error) = ssl.connection.process_new_packets() {
                eprintln!("connection {slot} SSL handshake failed: {error}");
                self.connections.remove(slot);
                return Ok(());
            }
            let mut plaintext = Vec::new();
            if let Err(error) = ssl.connection.reader().read_to_end(&mut plaintext) {
                if error.kind() != io::ErrorKind::WouldBlock {
                    eprintln!("connection {slot} SSL plaintext read failed: {error}");
                    self.connections.remove(slot);
                    return Ok(());
                }
            }
            if ssl.connection.wants_write() {
                self.submit_send(ConnectionId {
                    slot: operation.slot,
                    generation: operation.generation,
                })?;
            }
            plaintext
        } else {
            received
        };
        if received.is_empty() {
            return self.submit_recv(ConnectionId {
                slot: operation.slot,
                generation: operation.generation,
            });
        }
        let received_len = received.len();
        self.metrics.read_bytes(received_len);
        if connection
            .request_buffer
            .len()
            .checked_add(received_len)
            .is_none_or(|length| length > self.limits.max_read_buffer_size)
        {
            eprintln!("connection {slot} request exceeded the configured read limit");
            return self.queue_http_error(operation, 413, "request is too large");
        }
        connection.request_buffer.extend_from_slice(&received);

        if connection.pending_request.is_none() {
            match parse_request_head(&connection.request_buffer) {
                Ok(RequestHeadParse::Incomplete) => {}
                Ok(RequestHeadParse::Complete { request, consumed }) => {
                    let body_length = match request.body_length() {
                        Ok(length) => length,
                        Err(BodyFramingError::UnsupportedTransferEncoding) => {
                            eprintln!("connection {slot} used unsupported transfer encoding");
                            return self.queue_http_error(
                                operation,
                                501,
                                "transfer encoding is not supported",
                            );
                        }
                        Err(
                            BodyFramingError::InvalidContentLength
                            | BodyFramingError::ConflictingContentLength,
                        ) => {
                            eprintln!("connection {slot} sent an invalid content length");
                            return self.queue_http_error(operation, 400, "invalid content length");
                        }
                    };
                    let Some(body_end) = consumed.checked_add(body_length) else {
                        return self.queue_http_error(operation, 413, "request is too large");
                    };
                    if body_end > self.limits.max_read_buffer_size {
                        eprintln!("connection {slot} request body exceeded the configured limit");
                        return self.queue_http_error(operation, 413, "request is too large");
                    }
                    connection.pending_request = Some(PendingRequest {
                        head: request,
                        body_start: consumed,
                        body_end,
                    });
                }
                Err(error) => {
                    eprintln!("connection {slot} sent an invalid HTTP request: {error}");
                    return self.queue_http_error(operation, 400, "invalid HTTP request");
                }
            }
        }

        let request_complete = connection
            .pending_request
            .as_ref()
            .is_some_and(|pending| connection.request_buffer.len() >= pending.body_end);
        if request_complete {
            let pending = connection.pending_request.take().unwrap();
            connection.request = Some(CompletedRequest {
                head: pending.head,
                body: connection.request_buffer[pending.body_start..pending.body_end].to_vec(),
            });
            self.metrics.request();
            return self.prepare_response(ConnectionId {
                slot: operation.slot,
                generation: operation.generation,
            });
        }

        self.submit_recv(ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        })
    }

    fn queue_http_error(
        &mut self,
        operation: OperationId,
        status: u16,
        message: &str,
    ) -> io::Result<()> {
        self.metrics.error();
        let connection_id = ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        };
        let Some(connection) = self.connections.get_mut(operation.slot as usize) else {
            return Ok(());
        };
        if !connection.matches_generation(operation.generation) {
            return Ok(());
        }
        connection.pending_request = None;
        connection.queue_response(response_bytes(status, message));
        self.submit_send(connection_id)
    }

    fn prepare_response(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let slot = connection_id.slot as usize;
        let Some(connection) = self.connections.get(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(connection_id.generation) {
            return Ok(());
        }
        let Some(request) = connection.request.as_ref() else {
            return Ok(());
        };
        let Some(listener) = self.listeners.get(connection.listener_index) else {
            self.connections.remove(slot);
            return Ok(());
        };
        let request_head = request.head.clone();
        let request_body = request.body.clone();
        let server_index = select_server(
            &listener.server_indices,
            listener.default_server,
            &request_head,
            &self.servers,
        );
        let action = self
            .servers
            .get(server_index)
            .and_then(|server| route(server, &request_head.target))
            .cloned();
        let response = match action {
            Some(Action::Response { status, body }) => response_bytes(status, &body),
            Some(Action::Static { directory }) => {
                return match static_stream_response(directory.as_ref(), &request_head) {
                    Ok(response) => self.queue_static_response(connection_id, response),
                    Err(error) => {
                        self.connections[slot].queue_response(static_error_response(error));
                        self.submit_send(connection_id)
                    }
                };
            }
            Some(action @ Action::Proxy { .. }) => {
                let upstream = match self.balancer.select(&action) {
                    Ok(upstream) => upstream,
                    Err(error) => {
                        eprintln!("invalid upstream group: {error}");
                        return self.queue_proxy_error(
                            connection_id,
                            502,
                            "upstream configuration failed",
                        );
                    }
                };
                return self.start_proxy(connection_id, &upstream, &request_head, &request_body);
            }
            None if self.servers.get(server_index).is_none() => {
                response_bytes(500, "server configuration is unavailable")
            }
            None => response_bytes(404, "not found"),
        };
        if response.len() > self.limits.max_write_buffer_size {
            self.connections[slot].queue_response(response_bytes(500, "response is too large"));
        } else {
            self.connections[slot].queue_response(response);
        }
        self.submit_send(connection_id)
    }

    fn start_proxy(
        &mut self,
        connection_id: ConnectionId,
        upstream_url: &str,
        request: &RequestHead,
        body: &[u8],
    ) -> io::Result<()> {
        let upstream = match Upstream::parse(upstream_url) {
            Ok(upstream) => upstream,
            Err(error) => {
                eprintln!("invalid upstream URL: {error}");
                return self.queue_proxy_error(connection_id, 502, "upstream configuration failed");
            }
        };
        let request_buffer = upstream.request_bytes(request, body);
        if request_buffer.len() > self.limits.max_read_buffer_size {
            return self.queue_proxy_error(connection_id, 502, "upstream request is too large");
        }
        let address = upstream.connect_address().to_owned();
        let Some(connection) = self.connections.get_mut(connection_id.slot as usize) else {
            return Ok(());
        };
        connection.proxy = Some(UringProxy::resolving(request_buffer));
        if let Err(error) = self.resolver.resolve(connection_id, address) {
            eprintln!("failed to schedule upstream DNS resolution: {error}");
            return self.queue_proxy_error(connection_id, 502, "upstream DNS resolution failed");
        }
        Ok(())
    }

    fn submit_dns_read(&mut self) -> io::Result<()> {
        let entry = opcode::Read::new(
            types::Fd(self.dns_eventfd.as_raw_fd()),
            (&mut *self.dns_value as *mut u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(super::operation::DNS_USER_DATA);
        push_entry(&mut self.ring, &entry)?;
        Ok(())
    }

    fn process_dns_results(&mut self) -> io::Result<()> {
        for result in self.resolver.drain() {
            self.finish_resolution(result)?;
        }
        if !self.shutting_down {
            self.submit_dns_read()?;
        }
        Ok(())
    }

    fn finish_resolution(&mut self, result: ResolveResult) -> io::Result<()> {
        let connection_id = result.connection_id;
        let slot = connection_id.slot as usize;
        let Some(connection) = self.connections.get(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(connection_id.generation)
            || connection
                .proxy
                .as_ref()
                .is_none_or(|proxy| proxy.phase != ProxyPhase::Resolving)
        {
            return Ok(());
        }
        let addresses = match result.addresses {
            Ok(addresses) if !addresses.is_empty() => addresses,
            Ok(_) => return self.queue_proxy_error(connection_id, 502, "upstream has no address"),
            Err(error) => {
                eprintln!("upstream DNS resolution failed: {error}");
                return self.queue_proxy_error(
                    connection_id,
                    502,
                    "upstream DNS resolution failed",
                );
            }
        };
        let proxy = self.connections[slot].proxy.as_mut().unwrap();
        proxy.addresses = addresses.into();
        proxy.transition(ProxyPhase::Connecting);
        self.try_next_proxy_address(connection_id)
    }

    fn try_next_proxy_address(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        loop {
            let Some(address) = self.connections[connection_id.slot as usize]
                .proxy
                .as_mut()
                .and_then(|proxy| proxy.addresses.pop_front())
            else {
                return self.queue_proxy_error(connection_id, 502, "upstream connection failed");
            };
            let socket = match create_upstream_socket(address) {
                Ok(socket) => socket,
                Err(error) => {
                    eprintln!("failed to create upstream socket for {address}: {error}");
                    continue;
                }
            };
            let proxy = self.connections[connection_id.slot as usize]
                .proxy
                .as_mut()
                .unwrap();
            proxy.upstream = Some(socket);
            proxy.address = Some(Box::new(SockAddr::from(address)));
            proxy.record_progress();
            return self.submit_proxy_connect(connection_id);
        }
    }

    fn submit_proxy_connect(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let proxy = self.connections[connection_id.slot as usize]
            .proxy
            .as_mut()
            .unwrap();
        let socket = proxy.upstream.as_ref().unwrap();
        let address = proxy.address.as_ref().unwrap();
        let entry = opcode::Connect::new(
            types::Fd(socket.as_raw_fd()),
            address.as_ptr(),
            address.len(),
        )
        .build()
        .user_data(
            OperationId::proxy_connect(connection_id.slot, connection_id.generation).encode(),
        );
        push_entry(&mut self.ring, &entry)?;
        proxy.operation_pending = true;
        Ok(())
    }

    fn submit_proxy_write(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let proxy = self.connections[connection_id.slot as usize]
            .proxy
            .as_mut()
            .unwrap();
        let remaining = &proxy.request_buffer[proxy.request_offset..];
        let length = u32::try_from(remaining.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "upstream request is too large")
        })?;
        let entry = opcode::Send::new(
            types::Fd(proxy.upstream.as_ref().unwrap().as_raw_fd()),
            remaining.as_ptr(),
            length,
        )
        .flags(libc::MSG_NOSIGNAL)
        .build()
        .user_data(OperationId::proxy_write(connection_id.slot, connection_id.generation).encode());
        push_entry(&mut self.ring, &entry)?;
        proxy.operation_pending = true;
        Ok(())
    }

    fn submit_proxy_read(&mut self, connection_id: ConnectionId) -> io::Result<()> {
        let proxy = self.connections[connection_id.slot as usize]
            .proxy
            .as_mut()
            .unwrap();
        let length = u32::try_from(self.buffer_size).unwrap();
        let entry = opcode::Recv::new(
            types::Fd(proxy.upstream.as_ref().unwrap().as_raw_fd()),
            std::ptr::null_mut(),
            length,
        )
        .buf_group(BUFFER_GROUP)
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(OperationId::proxy_read(connection_id.slot, connection_id.generation).encode());
        push_entry(&mut self.ring, &entry)?;
        proxy.operation_pending = true;
        Ok(())
    }

    fn handle_proxy_connect(&mut self, operation: OperationId, result: i32) -> io::Result<()> {
        let connection_id = ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        };
        let Some(proxy) = self.current_proxy_mut(connection_id) else {
            return Ok(());
        };
        proxy.operation_pending = false;
        if proxy.timed_out {
            return self.fail_proxy(connection_id, 504, "upstream connect timed out");
        }
        if result < 0 {
            let error = io::Error::from_raw_os_error(-result);
            eprintln!("upstream connect attempt failed: {error}");
            return self.try_next_proxy_address(connection_id);
        }
        proxy.transition(ProxyPhase::WritingRequest);
        self.submit_proxy_write(connection_id)
    }

    fn handle_proxy_write(&mut self, operation: OperationId, result: i32) -> io::Result<()> {
        let metrics = self.metrics.clone();
        let connection_id = ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        };
        let Some(proxy) = self.current_proxy_mut(connection_id) else {
            return Ok(());
        };
        proxy.operation_pending = false;
        if proxy.timed_out {
            return self.fail_proxy(connection_id, 504, "upstream request write timed out");
        }
        if result < 0 {
            let error = io::Error::from_raw_os_error(-result);
            if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
                return self.submit_proxy_write(connection_id);
            }
            return self.fail_proxy(connection_id, 502, "upstream request write failed");
        }
        if result == 0 {
            return self.fail_proxy(connection_id, 502, "upstream request write failed");
        }
        let written = result as usize;
        metrics.wrote_bytes(written);
        if written > proxy.request_buffer.len() - proxy.request_offset {
            return self.fail_proxy(connection_id, 502, "invalid upstream write completion");
        }
        proxy.request_offset += written;
        proxy.record_progress();
        if proxy.request_offset == proxy.request_buffer.len() {
            proxy.transition(ProxyPhase::ReadingResponse);
            self.submit_proxy_read(connection_id)
        } else {
            self.submit_proxy_write(connection_id)
        }
    }

    fn handle_proxy_read(
        &mut self,
        operation: OperationId,
        result: i32,
        buffer_id: Option<u16>,
    ) -> io::Result<()> {
        let metrics = self.metrics.clone();
        let received = if result > 0 {
            let id = buffer_id.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "upstream receive completed without a selected buffer",
                )
            })?;
            Some(self.buffer_ring.copy_and_release(id, result as usize)?)
        } else {
            if let Some(id) = buffer_id {
                self.buffer_ring.copy_and_release(id, 0)?;
            }
            None
        };
        let connection_id = ConnectionId {
            slot: operation.slot,
            generation: operation.generation,
        };
        let max_write_buffer_size = self.limits.max_write_buffer_size;
        let Some(proxy) = self.current_proxy_mut(connection_id) else {
            return Ok(());
        };
        proxy.operation_pending = false;
        if proxy.timed_out {
            return self.fail_proxy(connection_id, 504, "upstream response timed out");
        }
        if result < 0 {
            let error = io::Error::from_raw_os_error(-result);
            if matches!(
                error.raw_os_error(),
                Some(libc::EAGAIN | libc::EINTR | libc::ENOBUFS)
            ) {
                return self.submit_proxy_read(connection_id);
            }
            return self.fail_proxy(connection_id, 502, "upstream response failed");
        }
        if result == 0 {
            self.metrics.response();
            self.connections.remove(operation.slot as usize);
            return Ok(());
        }
        let bytes = received.unwrap();
        metrics.read_bytes(bytes.len());
        if bytes.len() > max_write_buffer_size {
            return self.fail_proxy(connection_id, 502, "upstream response chunk is too large");
        }
        proxy.response_started = true;
        proxy.record_progress();
        self.connections[operation.slot as usize].queue_response(bytes);
        self.submit_send(connection_id)
    }

    fn current_proxy_mut(&mut self, connection_id: ConnectionId) -> Option<&mut UringProxy> {
        let connection = self.connections.get_mut(connection_id.slot as usize)?;
        connection
            .matches_generation(connection_id.generation)
            .then_some(())?;
        connection.proxy.as_mut()
    }

    fn queue_proxy_error(
        &mut self,
        connection_id: ConnectionId,
        status: u16,
        message: &str,
    ) -> io::Result<()> {
        self.metrics.error();
        let slot = connection_id.slot as usize;
        let Some(connection) = self.connections.get_mut(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(connection_id.generation) {
            return Ok(());
        }
        connection.proxy = None;
        connection.queue_response(response_bytes(status, message));
        self.submit_send(connection_id)
    }

    fn fail_proxy(
        &mut self,
        connection_id: ConnectionId,
        status: u16,
        message: &str,
    ) -> io::Result<()> {
        let slot = connection_id.slot as usize;
        let response_started = self
            .connections
            .get(slot)
            .and_then(|connection| connection.proxy.as_ref())
            .is_some_and(|proxy| proxy.response_started);
        if response_started {
            self.connections.remove(slot);
            Ok(())
        } else {
            self.queue_proxy_error(connection_id, status, message)
        }
    }

    fn submit_shutdown_read(&mut self) -> io::Result<()> {
        let entry = opcode::Read::new(
            types::Fd(self.shutdown_eventfd.as_raw_fd()),
            (&mut *self.shutdown_value as *mut u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(super::operation::CONTROL_USER_DATA);
        push_entry(&mut self.ring, &entry)?;
        Ok(())
    }

    fn submit_timer_read(&mut self) -> io::Result<()> {
        let entry = opcode::Read::new(
            types::Fd(self.timerfd.as_raw_fd()),
            (&mut *self.timer_value as *mut u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(super::operation::TIMER_USER_DATA);
        push_entry(&mut self.ring, &entry)?;
        Ok(())
    }

    fn check_proxy_timeouts(&mut self) -> io::Result<()> {
        let now = std::time::Instant::now();
        let expired: Vec<(ConnectionId, ProxyPhase, bool)> = self
            .connections
            .iter()
            .filter_map(|(slot, connection)| {
                if connection.write_pending {
                    return None;
                }
                let proxy = connection.proxy.as_ref()?;
                if proxy.timed_out {
                    return None;
                }
                let timeout = match proxy.phase {
                    ProxyPhase::Resolving => self.dns_limits.timeout,
                    ProxyPhase::Connecting => self.proxy_limits.connect_timeout,
                    ProxyPhase::WritingRequest => self.proxy_limits.write_timeout,
                    ProxyPhase::ReadingResponse => self.proxy_limits.read_timeout,
                };
                (now.duration_since(proxy.progress_at) >= timeout).then_some((
                    ConnectionId {
                        slot: slot as u32,
                        generation: connection.generation,
                    },
                    proxy.phase,
                    proxy.operation_pending,
                ))
            })
            .collect();

        for (connection_id, phase, operation_pending) in expired {
            if !operation_pending {
                self.fail_proxy(connection_id, 504, phase.timeout_message())?;
                continue;
            }
            let proxy = self.connections[connection_id.slot as usize]
                .proxy
                .as_mut()
                .unwrap();
            proxy.timed_out = true;
            let target = match phase {
                ProxyPhase::Resolving => continue,
                ProxyPhase::Connecting => {
                    OperationId::proxy_connect(connection_id.slot, connection_id.generation)
                }
                ProxyPhase::WritingRequest => {
                    OperationId::proxy_write(connection_id.slot, connection_id.generation)
                }
                ProxyPhase::ReadingResponse => {
                    OperationId::proxy_read(connection_id.slot, connection_id.generation)
                }
            };
            let entry = opcode::AsyncCancel::new(target.encode())
                .build()
                .user_data(super::operation::PROXY_CANCEL_USER_DATA);
            push_entry(&mut self.ring, &entry)?;
        }
        let handshake_expired: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(slot, connection)| {
                connection.ssl.as_ref().and_then(|ssl| {
                    (ssl.connection.is_handshaking()
                        && now.duration_since(ssl.started_at) >= ssl.handshake_timeout)
                        .then_some(ConnectionId {
                            slot: slot as u32,
                            generation: connection.generation,
                        })
                })
            })
            .collect();
        for connection_id in handshake_expired {
            eprintln!(
                "SSL handshake timed out for connection {}",
                connection_id.slot
            );
            self.connections.remove(connection_id.slot as usize);
        }
        // The periodic timer also wakes a draining worker so its graceful
        // shutdown deadline is enforced even when clients are otherwise idle.
        self.submit_timer_read()?;
        self.pump_static_streams()?;
        Ok(())
    }

    fn queue_static_response(
        &mut self,
        connection_id: ConnectionId,
        response: crate::static_files::StaticStreamResponse,
    ) -> io::Result<()> {
        let connection = &mut self.connections[connection_id.slot as usize];
        connection.static_stream = response.stream;
        connection.queue_response(response.head);
        self.submit_send(connection_id)
    }

    fn pump_static_streams(&mut self) -> io::Result<()> {
        let ready: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(slot, connection)| {
                (connection.static_stream.is_some()
                    && !connection.write_pending
                    && connection.write_buffer.is_empty())
                .then_some(ConnectionId {
                    slot: slot as u32,
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
        let next = self.connections[connection_id.slot as usize]
            .static_stream
            .as_ref()
            .unwrap()
            .try_next();
        match next {
            Ok(StaticChunk::Data(bytes)) => {
                self.connections[connection_id.slot as usize].queue_response(bytes);
                self.submit_send(connection_id)
            }
            Ok(StaticChunk::Pending) => Ok(()),
            Ok(StaticChunk::Finished) => {
                self.metrics.response();
                self.connections.remove(connection_id.slot as usize);
                Ok(())
            }
            Err(error) => {
                self.metrics.error();
                eprintln!("static file stream failed: {error}");
                self.connections.remove(connection_id.slot as usize);
                Ok(())
            }
        }
    }

    fn begin_shutdown(&mut self) -> io::Result<()> {
        if self.cancellation_started {
            return Ok(());
        }
        self.shutting_down = true;
        self.shutdown_started = Some(std::time::Instant::now());
        self.cancellation_started = true;

        let mut targets = Vec::new();
        for (index, listener) in self.listeners.iter().enumerate() {
            if listener.accept_pending() {
                targets.push(listener.accept_operation(index)?.encode());
            }
        }
        self.pending_cancellations = targets.len();
        for target in targets {
            let entry = opcode::AsyncCancel::new(target)
                .build()
                .user_data(super::operation::CANCEL_USER_DATA);
            push_entry(&mut self.ring, &entry)?;
        }
        self.ring.submit()?;
        Ok(())
    }

    fn shutdown_drained(&self) -> bool {
        self.pending_cancellations == 0
            && self
                .listeners
                .iter()
                .all(|listener| !listener.accept_pending())
            && self.connections.is_empty()
    }

    fn shutdown_deadline_reached(&self) -> bool {
        self.shutdown_started
            .is_some_and(|started| started.elapsed() >= self.limits.drain_timeout)
    }

    /// Submit initial operations and dispatch completions until shutdown.
    pub(super) fn run(mut self) -> io::Result<()> {
        let _owned_resources = (
            &self.ring,
            &self.listeners,
            &self.connections,
            &self.generations,
            &self.servers,
            &self.shutdown,
            &self.limits,
            &self.proxy_limits,
            &self.dns_limits,
        );
        self.submit_initial_accepts()?;
        self.submit_shutdown_read()?;
        self.submit_dns_read()?;
        self.submit_timer_read()?;
        self.ring.submit()?;

        loop {
            if self.shutdown.is_requested() {
                self.begin_shutdown()?;
            }
            if self.shutting_down && self.shutdown_drained() {
                return Ok(());
            }
            if self.shutting_down && self.shutdown_deadline_reached() {
                eprintln!(
                    "io_uring graceful shutdown deadline reached with {} active connection(s)",
                    self.connections.len()
                );
                return Ok(());
            }
            self.ring.submit_and_wait(1)?;
            let completions: Vec<(u64, i32, u32, Option<u16>)> = self
                .ring
                .completion()
                .map(|cqe| {
                    let flags = cqe.flags();
                    (
                        cqe.user_data(),
                        cqe.result(),
                        flags,
                        cqueue::buffer_select(flags),
                    )
                })
                .collect();

            for (user_data, result, flags, buffer_id) in completions {
                if user_data == super::operation::CONTROL_USER_DATA {
                    self.shutdown.request();
                    continue;
                }
                if user_data == super::operation::CANCEL_USER_DATA {
                    self.pending_cancellations = self.pending_cancellations.saturating_sub(1);
                    continue;
                }
                if user_data == super::operation::PROXY_CANCEL_USER_DATA {
                    continue;
                }
                if user_data == super::operation::DNS_USER_DATA {
                    self.process_dns_results()?;
                    continue;
                }
                if user_data == super::operation::TIMER_USER_DATA {
                    self.check_proxy_timeouts()?;
                    continue;
                }
                let Some(operation) = OperationId::decode(user_data) else {
                    if let Some(id) = buffer_id {
                        self.buffer_ring
                            .copy_and_release(id, result.max(0) as usize)?;
                    }
                    eprintln!("ignoring invalid io_uring completion identifier {user_data}");
                    continue;
                };
                self.dispatch_completion(Completion {
                    operation,
                    result,
                    flags,
                    buffer_id,
                })?;
            }
            self.ring.submit()?;
        }
    }
}

/// Queue an operation without treating temporary SQ exhaustion as a worker
/// failure. Submitting the current batch advances the shared SQ head, after
/// which the same entry can be scheduled safely.
fn push_entry(
    ring: &mut IoUring<squeue::Entry, cqueue::Entry>,
    entry: &squeue::Entry,
) -> io::Result<()> {
    loop {
        if unsafe { ring.submission().push(entry) }.is_ok() {
            return Ok(());
        }
        ring.submit()?;
    }
}

pub(super) fn multishot_is_unsupported(result: i32) -> bool {
    result < 0 && matches!(-result, libc::EINVAL | libc::EOPNOTSUPP)
}

fn create_eventfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn create_timerfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    let interval = libc::timespec {
        tv_sec: 0,
        tv_nsec: 50_000_000,
    };
    let timer = libc::itimerspec {
        it_interval: interval,
        it_value: interval,
    };
    if unsafe { libc::timerfd_settime(owned.as_raw_fd(), 0, &timer, std::ptr::null_mut()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(owned)
}

fn create_upstream_socket(address: std::net::SocketAddr) -> io::Result<OwnedFd> {
    let domain = if address.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    let fd = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}
