use super::{
    connection::{
        next_generation, CompletedRequest, ConnectionId, PendingRequest, UringConnection,
    },
    listener::UringListener,
    operation::{OperationId, OperationKind},
    IoUringRuntime,
};
use crate::{
    parse_request_head, response_bytes, route, select_server, BodyFramingError, DnsLimits,
    ProxyLimits, RequestHeadParse, ShutdownHandle, WorkerContext, WorkerLimits,
};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use proxy_common::Action;
use proxy_common::Server;
use slab::Slab;
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::Arc,
};

pub struct IoUringWorker {
    ring: IoUring<squeue::Entry, cqueue::Entry>,
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
    cancellation_started: bool,
    pending_cancellations: usize,
}

struct Completion {
    operation: OperationId,
    result: i32,
}

impl IoUringWorker {
    /// Build the ring and take ownership of every listener in the worker context.
    pub(super) fn new(runtime: IoUringRuntime, context: WorkerContext) -> io::Result<Self> {
        let buffer_size = runtime.buf_size;
        let ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
            .setup_cqsize(runtime.cq_entries)
            .build(runtime.sq_entries)?;

        let listeners = context
            .listener_groups
            .into_iter()
            .map(|group| {
                UringListener::new(
                    group.socket.into(),
                    group.default_server,
                    group.server_indices,
                )
            })
            .collect();
        let shutdown_eventfd = Arc::new(create_eventfd()?);
        context
            .shutdown
            .install_eventfd(Arc::clone(&shutdown_eventfd));

        Ok(Self {
            ring,
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
            cancellation_started: false,
            pending_cancellations: 0,
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

        unsafe { self.ring.submission().push(&entry) }.map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue is full",
            )
        })?;

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
            OperationKind::Accept => {
                self.handle_accept_completion(completion.operation, completion.result)
            }
            OperationKind::Read => {
                self.handle_read_completion(completion.operation, completion.result)
            }
            OperationKind::Write => {
                self.handle_write_completion(completion.operation, completion.result)
            }
        }
    }

    fn handle_accept_completion(&mut self, operation: OperationId, result: i32) -> io::Result<()> {
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
        listener.mark_accept_completed();

        if let Some(accepted) = accepted {
            if !self.shutting_down {
                match self.insert_connection(accepted, listener_idx) {
                    Ok(connection_id) => self.submit_recv(connection_id)?,
                    Err(error) => eprintln!("failed to retain accepted connection: {error}"),
                }
            }
        } else {
            let error = io::Error::from_raw_os_error(-result);
            eprintln!("listener {listener_idx} accept failed: {error}");
        }

        if !self.shutdown.is_requested() {
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
        let remaining = &connection.write_buffer[connection.write_offset..];
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

        unsafe { self.ring.submission().push(&entry) }.map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue is full",
            )
        })?;
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
        if self.shutting_down {
            self.connections.remove(slot);
            return Ok(());
        }

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
        let remaining = connection.write_buffer.len() - connection.write_offset;
        if written > remaining {
            eprintln!("connection {slot} returned an invalid send length");
            self.connections.remove(slot);
            return Ok(());
        }
        connection.write_offset += written;
        if connection.write_offset == connection.write_buffer.len() {
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
        let connection = entry.insert(UringConnection::new(
            socket,
            generation,
            listener_index,
            self.buffer_size,
        ));

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
        let read_len = u32::try_from(connection.read_buffer.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "receive buffer exceeds io_uring length capacity",
            )
        })?;
        let entry = opcode::Recv::new(
            types::Fd(connection.socket.as_raw_fd()),
            connection.read_buffer.as_mut_ptr(),
            read_len,
        )
        .build()
        .user_data(OperationId::read(connection_id.slot, connection_id.generation).encode());

        unsafe { self.ring.submission().push(&entry) }.map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue is full",
            )
        })?;
        connection.mark_read_submitted();
        Ok(())
    }

    fn handle_read_completion(&mut self, operation: OperationId, result: i32) -> io::Result<()> {
        let slot = operation.slot as usize;
        let Some(connection) = self.connections.get_mut(slot) else {
            return Ok(());
        };
        if !connection.matches_generation(operation.generation) {
            return Ok(());
        }

        if result <= 0 {
            if result < 0 {
                let error = io::Error::from_raw_os_error(-result);
                eprintln!("connection {slot} receive failed: {error}");
            }
            self.connections.remove(slot);
            return Ok(());
        }

        let received_len = result as usize;
        if received_len > connection.read_buffer.len() {
            eprintln!("connection {slot} returned an invalid receive length");
            self.connections.remove(slot);
            return Ok(());
        }
        connection.mark_read_completed();
        if self.shutting_down {
            self.connections.remove(slot);
            return Ok(());
        }
        if connection.request_buffer.len() + received_len > self.limits.max_read_buffer_size {
            eprintln!("connection {slot} request exceeded the configured read limit");
            self.connections.remove(slot);
            return Ok(());
        }
        connection
            .request_buffer
            .extend_from_slice(&connection.read_buffer[..received_len]);

        if connection.pending_request.is_none() {
            match parse_request_head(&connection.request_buffer) {
                Ok(RequestHeadParse::Incomplete) => {}
                Ok(RequestHeadParse::Complete { request, consumed }) => {
                    let body_length = match request.body_length() {
                        Ok(length) => length,
                        Err(BodyFramingError::UnsupportedTransferEncoding) => {
                            eprintln!("connection {slot} used unsupported transfer encoding");
                            self.connections.remove(slot);
                            return Ok(());
                        }
                        Err(
                            BodyFramingError::InvalidContentLength
                            | BodyFramingError::ConflictingContentLength,
                        ) => {
                            eprintln!("connection {slot} sent an invalid content length");
                            self.connections.remove(slot);
                            return Ok(());
                        }
                    };
                    let Some(body_end) = consumed.checked_add(body_length) else {
                        self.connections.remove(slot);
                        return Ok(());
                    };
                    if body_end > self.limits.max_read_buffer_size {
                        eprintln!("connection {slot} request body exceeded the configured limit");
                        self.connections.remove(slot);
                        return Ok(());
                    }
                    connection.pending_request = Some(PendingRequest {
                        head: request,
                        body_start: consumed,
                        body_end,
                    });
                }
                Err(error) => {
                    eprintln!("connection {slot} sent an invalid HTTP request: {error}");
                    self.connections.remove(slot);
                    return Ok(());
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
        let server_index = select_server(
            &listener.server_indices,
            listener.default_server,
            &request.head,
            &self.servers,
        );
        let response = match self.servers.get(server_index) {
            Some(server) => match route(server, &request.head.target) {
                Some(Action::Response { status, body }) => response_bytes(*status, body),
                Some(Action::Static { .. }) => {
                    response_bytes(501, "static action is not implemented by io_uring")
                }
                Some(Action::Proxy { .. }) => {
                    response_bytes(501, "proxy action is not implemented by io_uring")
                }
                None => response_bytes(404, "not found"),
            },
            None => response_bytes(500, "server configuration is unavailable"),
        };
        if response.len() > self.limits.max_write_buffer_size {
            self.connections[slot].queue_response(response_bytes(500, "response is too large"));
        } else {
            self.connections[slot].queue_response(response);
        }
        self.submit_send(connection_id)
    }

    fn submit_shutdown_read(&mut self) -> io::Result<()> {
        let entry = opcode::Read::new(
            types::Fd(self.shutdown_eventfd.as_raw_fd()),
            (&mut *self.shutdown_value as *mut u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
        .build()
        .user_data(super::operation::CONTROL_USER_DATA);
        unsafe { self.ring.submission().push(&entry) }.map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue is full",
            )
        })?;
        Ok(())
    }

    fn begin_shutdown(&mut self) -> io::Result<()> {
        if self.cancellation_started {
            return Ok(());
        }
        self.shutting_down = true;
        self.cancellation_started = true;

        let mut targets = Vec::new();
        for (index, listener) in self.listeners.iter().enumerate() {
            if listener.accept_pending() {
                targets.push(listener.accept_operation(index)?.encode());
            }
        }
        let mut idle_connections = Vec::new();
        for (slot, connection) in &self.connections {
            let operation_slot = u32::try_from(slot).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "connection slot is too large")
            })?;
            if connection.read_pending {
                targets.push(OperationId::read(operation_slot, connection.generation).encode());
            } else if connection.write_pending {
                targets.push(OperationId::write(operation_slot, connection.generation).encode());
            } else {
                idle_connections.push(slot);
            }
        }
        for slot in idle_connections {
            self.connections.remove(slot);
        }

        self.pending_cancellations = targets.len();
        for target in targets {
            let entry = opcode::AsyncCancel::new(target)
                .build()
                .user_data(super::operation::CANCEL_USER_DATA);
            loop {
                if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                    break;
                }
                self.ring.submit()?;
            }
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
        self.ring.submit()?;

        loop {
            if self.shutdown.is_requested() {
                self.begin_shutdown()?;
            }
            if self.shutting_down && self.shutdown_drained() {
                return Ok(());
            }
            self.ring.submit_and_wait(1)?;
            let completions: Vec<(u64, i32)> = self
                .ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect();

            for (user_data, result) in completions {
                if user_data == super::operation::CONTROL_USER_DATA {
                    self.shutdown.request();
                    continue;
                }
                if user_data == super::operation::CANCEL_USER_DATA {
                    self.pending_cancellations = self.pending_cancellations.saturating_sub(1);
                    continue;
                }
                let Some(operation) = OperationId::decode(user_data) else {
                    eprintln!("ignoring invalid io_uring completion identifier {user_data}");
                    continue;
                };
                self.dispatch_completion(Completion { operation, result })?;
            }
            self.ring.submit()?;
        }
    }
}

fn create_eventfd() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}
