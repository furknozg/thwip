use super::{
    listener::UringListener,
    operation::{OperationId, OperationKind},
    IoUringRuntime,
};
use crate::{DnsLimits, ProxyLimits, ShutdownHandle, WorkerContext, WorkerLimits};
use io_uring::{cqueue, squeue, IoUring};
use proxy_common::Server;
use std::{
    io,
    os::fd::{FromRawFd, OwnedFd},
};

pub struct IoUringWorker {
    ring: IoUring<squeue::Entry, cqueue::Entry>,
    listeners: Vec<UringListener>,
    servers: Vec<Server>,
    shutdown: ShutdownHandle,
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
            OperationKind::Read | OperationKind::Write => {
                // these are not submitted
                Ok(())
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
            // Connection storage and recv submission are the next milestone.
            drop(accepted);
        } else {
            let error = io::Error::from_raw_os_error(-result);
            eprintln!("listener {listener_idx} accept failed: {error}");
        }

        if !self.shutdown.is_requested() {
            self.submit_accept(listener_idx)?;
        }
        Ok(())
    }

    /// Submit initial operations and dispatch completions until shutdown.
    pub(super) fn run(mut self) -> io::Result<()> {
        let _owned_resources = (
            &self.ring,
            &self.listeners,
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
