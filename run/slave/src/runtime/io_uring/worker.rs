use super::{
    connection::{next_generation, ConnectionId, UringConnection},
    listener::UringListener,
    operation::{OperationId, OperationKind},
    IoUringRuntime,
};
use crate::{DnsLimits, ProxyLimits, ShutdownHandle, WorkerContext, WorkerLimits};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use proxy_common::Server;
use slab::Slab;
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
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
            OperationKind::Write => Ok(()),
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
            match self.insert_connection(accepted, listener_idx) {
                Ok(connection_id) => self.submit_recv(connection_id)?,
                Err(error) => eprintln!("failed to retain accepted connection: {error}"),
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
        connection.mark_read_completed(received_len);
        Ok(())
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

        while !self.shutdown.is_requested() {
            self.ring.submit_and_wait(1)?;
            // drain completion queue entries here
            let completions: Vec<Completion> = self
                .ring
                .completion()
                .filter_map(|cqe| {
                    OperationId::decode(cqe.user_data()).map(|operation| Completion {
                        operation,
                        result: cqe.result(),
                    })
                })
                .collect();

            for completion in completions {
                self.dispatch_completion(completion)?;
            }
            self.ring.submit()?;
        }
        Ok(())
    }
}
