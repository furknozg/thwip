use crate::BoundListenerGroup;
use proxy_common::{Server, WorkerConfig};
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct ShutdownState {
    requested: Arc<AtomicBool>,
    #[cfg(unix)]
    waker: Mutex<Option<Arc<mio::Waker>>>,
}

#[derive(Clone, Default)]
pub struct ShutdownHandle(Arc<ShutdownState>);

impl ShutdownHandle {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn request(&self) {
        self.0.requested.store(true, Ordering::Release);
        #[cfg(unix)]
        if let Ok(waker) = self.0.waker.lock() {
            if let Some(waker) = waker.as_ref() {
                let _ = waker.wake();
            }
        }
    }
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0.requested)
    }
    pub fn is_requested(&self) -> bool {
        self.0.requested.load(Ordering::Acquire)
    }

    #[cfg(unix)]
    pub(crate) fn install_waker(&self, waker: Arc<mio::Waker>) {
        if let Ok(mut installed) = self.0.waker.lock() {
            *installed = Some(waker);
        }
    }
}

pub struct WorkerContext {
    pub listener_groups: Vec<BoundListenerGroup>,
    pub servers: Vec<Server>,
    pub shutdown: ShutdownHandle,
    pub limits: WorkerLimits,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerLimits {
    pub max_connections: usize,
    pub max_read_buffer_size: usize,
    pub max_write_buffer_size: usize,
    pub idle_timeout: Duration,
    pub drain_timeout: Duration,
}

impl WorkerLimits {
    pub fn from_config(config: &WorkerConfig) -> Self {
        Self {
            max_connections: config.max_connections,
            max_read_buffer_size: config.max_read_buffer_size,
            max_write_buffer_size: config.max_write_buffer_size,
            idle_timeout: Duration::from_millis(config.idle_timeout_ms),
            drain_timeout: Duration::from_millis(config.drain_timeout_ms),
        }
    }
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self::from_config(&WorkerConfig::default())
    }
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
