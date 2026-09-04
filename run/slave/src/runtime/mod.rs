use crate::BoundListenerGroup;
use proxy_common::{DnsConfig, ProxyTimeoutConfig, Server, WorkerConfig};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, OwnedFd};
use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct ShutdownState {
    requested: Arc<AtomicBool>,
    #[cfg(unix)]
    waker: Mutex<Option<Arc<mio::Waker>>>,
    #[cfg(target_os = "linux")]
    eventfd: Mutex<Option<Arc<OwnedFd>>>,
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
        #[cfg(target_os = "linux")]
        if let Ok(eventfd) = self.0.eventfd.lock() {
            if let Some(eventfd) = eventfd.as_ref() {
                let value = 1_u64;
                unsafe {
                    libc::write(
                        eventfd.as_raw_fd(),
                        (&value as *const u64).cast(),
                        std::mem::size_of::<u64>(),
                    );
                }
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

    #[cfg(target_os = "linux")]
    pub(crate) fn install_eventfd(&self, eventfd: Arc<OwnedFd>) {
        if let Ok(mut installed) = self.0.eventfd.lock() {
            *installed = Some(eventfd);
        }
        if self.is_requested() {
            self.request();
        }
    }
}

pub struct WorkerContext {
    pub listener_groups: Vec<BoundListenerGroup>,
    pub servers: Vec<Server>,
    pub shutdown: ShutdownHandle,
    pub limits: WorkerLimits,
    pub proxy_limits: ProxyLimits,
    pub dns_limits: DnsLimits,
    pub metrics: WorkerMetrics,
    pub upstream_groups: HashMap<String, proxy_common::UpstreamGroup>,
}

#[derive(Clone, Default)]
pub struct WorkerMetrics(Arc<WorkerMetricCounters>);

#[derive(Default)]
struct WorkerMetricCounters {
    accepted: AtomicU64,
    requests: AtomicU64,
    responses: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    errors: AtomicU64,
}

impl WorkerMetrics {
    pub(crate) fn accepted(&self) {
        self.0.accepted.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn request(&self) {
        self.0.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn response(&self) {
        self.0.responses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn read_bytes(&self, count: usize) {
        self.0.bytes_read.fetch_add(count as u64, Ordering::Relaxed);
    }
    pub(crate) fn wrote_bytes(&self, count: usize) {
        self.0
            .bytes_written
            .fetch_add(count as u64, Ordering::Relaxed);
    }
    pub(crate) fn error(&self) {
        self.0.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn report(&self, cpu_id: usize, runtime: &str, outcome: &str) {
        log::info!(
            "event=worker_shutdown cpu_id={cpu_id} runtime={runtime} outcome={outcome} accepted={} requests={} responses={} bytes_read={} bytes_written={} errors={}",
            self.0.accepted.load(Ordering::Relaxed),
            self.0.requests.load(Ordering::Relaxed),
            self.0.responses.load(Ordering::Relaxed),
            self.0.bytes_read.load(Ordering::Relaxed),
            self.0.bytes_written.load(Ordering::Relaxed),
            self.0.errors.load(Ordering::Relaxed),
        );
    }
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

#[derive(Debug, Clone, Copy)]
pub struct ProxyLimits {
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
}

impl ProxyLimits {
    pub fn from_config(config: &ProxyTimeoutConfig) -> Self {
        Self {
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            write_timeout: Duration::from_millis(config.write_timeout_ms),
            read_timeout: Duration::from_millis(config.read_timeout_ms),
        }
    }
}

impl Default for ProxyLimits {
    fn default() -> Self {
        Self::from_config(&ProxyTimeoutConfig::default())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DnsLimits {
    pub resolver_threads: usize,
    pub timeout: Duration,
}

impl DnsLimits {
    pub fn from_config(config: &DnsConfig) -> Self {
        Self {
            resolver_threads: config.resolver_threads,
            timeout: Duration::from_millis(config.timeout_ms),
        }
    }
}

impl Default for DnsLimits {
    fn default() -> Self {
        Self::from_config(&DnsConfig::default())
    }
}

pub trait Runtime {
    fn run(self, context: WorkerContext) -> io::Result<()>;
}

mod epoll;
pub use epoll::{run_epoll, run_epoll_with_shutdown, EpollRuntime};

mod readiness;

mod io_uring;
pub use io_uring::IoUringRuntime;

mod kqueue;
pub use kqueue::KqueueRuntime;
