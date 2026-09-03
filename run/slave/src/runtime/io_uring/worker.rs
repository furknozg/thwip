use super::IoUringRuntime;
use crate::{ShutdownHandle, WorkerContext};
use io_uring::IoUring;
use std::{io, os::fd::OwnedFd};

pub struct IoUringWorker {
    ring: IoUring,
    listeners: Vec<OwnedFd>,
    shutdown: ShutdownHandle,
}

impl IoUringWorker {
    /// Build the ring and take ownership of every listener in the worker context.
    pub(super) fn new(_runtime: IoUringRuntime, _context: WorkerContext) -> io::Result<Self> {
        todo!("construct the configured ring and listener state")
    }

    /// Submit initial operations and dispatch completions until shutdown.
    pub(super) fn run(self) -> io::Result<()> {
        let _owned_resources = (&self.ring, &self.listeners, &self.shutdown);
        todo!("run the io_uring submission and completion loop")
    }
}
