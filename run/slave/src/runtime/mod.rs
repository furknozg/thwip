use crate::BoundListenerGroup;
use proxy_common::Server;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Clone, Default)]
pub struct ShutdownHandle(Arc<AtomicBool>);

impl ShutdownHandle {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub fn request(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }
    pub fn is_requested(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct WorkerContext {
    pub listener_groups: Vec<BoundListenerGroup>,
    pub servers: Vec<Server>,
    pub shutdown: ShutdownHandle,
}

pub trait Runtime {
    fn run(self, context: WorkerContext) -> io::Result<()>;
}

mod epoll;
pub use epoll::{run_epoll, run_epoll_with_shutdown, EpollRuntime};

mod io_uring;
pub use io_uring::IoUringRuntime;

mod kqueue;
pub use kqueue::KqueueRuntime;
