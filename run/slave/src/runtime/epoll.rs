use super::{
    DnsLimits, ProxyLimits, Runtime, ShutdownHandle, WorkerContext, WorkerLimits, WorkerMetrics,
};
use crate::{load_ssl_configs, BoundListenerGroup};
use proxy_common::Server;
use std::io;

/// Linux runtime selector. Socket/event behavior lives in `readiness` so the
/// HTTP and connection lifecycle remains identical to kqueue.
pub struct EpollRuntime {
    pub max_events: usize,
}

impl Runtime for EpollRuntime {
    fn run(self, context: WorkerContext) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            super::readiness::run(context, self.max_events)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = context;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "epoll is only supported on Linux",
            ))
        }
    }
}

#[cfg(unix)]
pub fn run_epoll(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
) -> io::Result<()> {
    run_epoll_with_shutdown(listener_groups, servers, max_events, ShutdownHandle::new())
}

#[cfg(unix)]
pub fn run_epoll_with_shutdown(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
    shutdown: ShutdownHandle,
) -> io::Result<()> {
    let ssl_configs = load_ssl_configs(&servers)?;
    EpollRuntime { max_events }.run(WorkerContext {
        listener_groups,
        ssl_configs,
        servers,
        shutdown,
        limits: WorkerLimits::default(),
        proxy_limits: ProxyLimits::default(),
        dns_limits: DnsLimits::default(),
        metrics: WorkerMetrics::default(),
        upstream_groups: Default::default(),
    })
}

#[cfg(not(unix))]
pub fn run_epoll(
    _listener_groups: Vec<BoundListenerGroup>,
    _servers: Vec<Server>,
    _max_events: usize,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "epoll is only supported on Linux",
    ))
}

#[cfg(not(unix))]
pub fn run_epoll_with_shutdown(
    listener_groups: Vec<BoundListenerGroup>,
    servers: Vec<Server>,
    max_events: usize,
    _shutdown: ShutdownHandle,
) -> io::Result<()> {
    run_epoll(listener_groups, servers, max_events)
}
