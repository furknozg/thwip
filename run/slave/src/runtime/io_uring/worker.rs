use super::IoUringRuntime;
use crate::{DnsLimits, ProxyLimits, ShutdownHandle, WorkerContext, WorkerLimits, runtime::io_uring::{listener::UringListener, operation::{OperationId, OperationKind}}};
use io_uring::{IoUring, cqueue, opcode, squeue, types};
use proxy_common::Server;
use std::{io, os::fd::{AsRawFd, OwnedFd}};

pub struct IoUringWorker {
    ring: IoUring,
    listeners: Vec<UringListener>,
    servers: Vec<Server>,
    shutdown: ShutdownHandle,
    limits: WorkerLimits,
    proxy_limits: ProxyLimits,
    dns_limits: DnsLimits
}

struct Completion {
    operation : OperationId,
    result : i32
}

impl IoUringWorker {
    /// Build the ring and take ownership of every listener in the worker context.
    pub(super) fn new(runtime: IoUringRuntime, context: WorkerContext) -> io::Result<Self> {
        let ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
            .setup_cqsize(runtime.cq_entries)
            .build(runtime.sq_entries)?;

        let listeners = context.listener_groups.into_iter()
        .map(|g| UringListener{
            socket: g.socket.into(),
            default_server: g.default_server,
            server_indices : g.server_indices,
            generation : 1,
            accept_pending: false
        }).collect();

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

    pub fn submit_accept (&mut self, listener_idx : usize) -> io::Result<()> {
        // validate index
        let listener = &self.listeners[listener_idx];
        // avoid multiple attempts
        // build opid accept
        // build opcode accept encoding sqe
        // push it to sq
        // mark accept pending true
        let entry = opcode::Accept::new(
            types::Fd(listener.socket.as_raw_fd()),
            std::ptr::null_mut(),
            std::ptr::null_mut()
        ).build()
        .user_data(
            OperationId::accept(listener_idx as u32, listener.generation).encode(),
        );

        let push = unsafe {
            self.ring
                .submission()
                .push(&entry)
        };
        if push.is_err() { 
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock, 
                "io_uring submission queue is full"
            ));
        } else {
            return Ok(());
        }
    }

    pub fn submit_initial_accepts(&mut self) -> io::Result<()>{
        for listener_idx in 0..self.listeners.len() {
            self.submit_accept(listener_idx)?;
        }

        self.ring.submit()?;
        Ok(())
    }

    fn dispatch_completion(&mut self, completion : Completion ) -> io::Result<()> {
        match completion.operation.kind {
            OperationKind::Accept => { 
                self.handle_accept_completion(
                    completion.operation,
                    completion.result
                )
            }
            OperationKind::Read | OperationKind::Write => {
                // these are not submitted
                Ok(())
            }
        }
    }


    /// Submit initial operations and dispatch completions until shutdown.
    pub(super) fn run(mut self) -> io::Result<()> {
        let _owned_resources = (&self.ring, &self.listeners, &self.shutdown);
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
            }).collect();

            for completion in completions {
                self.dispatch_completion(completion)?;
            }
        }
        Ok(())
    }
}
