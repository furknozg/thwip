use proxy_common::{AsyncRuntimeConfig, Config};
use std::io;

use crate::{
    bind_worker_listeners, DnsLimits, EpollRuntime, IoUringRuntime, KqueueRuntime, ProxyLimits,
    Runtime, ShutdownHandle, WorkerContext, WorkerLimits, WorkerMetrics,
};

pub fn start_worker(cpu_id: usize, config: &Config) -> io::Result<()> {
    let shutdown = ShutdownHandle::new();
    install_shutdown_signals(&shutdown)?;
    pin_to_cpu(cpu_id)?;
    let runtime = select_runtime(&config.runtime);
    let runtime_name = runtime.name();
    let metrics = WorkerMetrics::default();
    let listener_groups = bind_worker_listeners(config)?;
    let context = WorkerContext {
        listener_groups,
        servers: config.http.servers.clone(),
        shutdown,
        limits: WorkerLimits::from_config(&config.worker),
        proxy_limits: ProxyLimits::from_config(&config.proxy),
        dns_limits: DnsLimits::from_config(&config.dns),
        metrics: metrics.clone(),
    };

    log::info!("event=worker_start cpu_id={cpu_id} runtime={runtime_name}");
    let result = runtime.run(context);
    metrics.report(
        cpu_id,
        runtime_name,
        if result.is_ok() { "clean" } else { "error" },
    );
    result
}

enum SelectedRuntime {
    Epoll(EpollRuntime),
    Kqueue(KqueueRuntime),
    IoUring(IoUringRuntime),
}

impl SelectedRuntime {
    fn name(&self) -> &'static str {
        match self {
            Self::Epoll(_) => "epoll",
            Self::Kqueue(_) => "kqueue",
            Self::IoUring(_) => "io_uring",
        }
    }

    fn run(self, context: WorkerContext) -> io::Result<()> {
        match self {
            Self::Epoll(runtime) => runtime.run(context),
            Self::Kqueue(runtime) => runtime.run(context),
            Self::IoUring(runtime) => runtime.run(context),
        }
    }
}

fn select_runtime(config: &AsyncRuntimeConfig) -> SelectedRuntime {
    match config {
        AsyncRuntimeConfig::Auto {
            max_events,
            sq_entries,
            cq_entries,
            buf_ring_size,
            buf_size,
        } => select_auto_runtime(
            *max_events,
            IoUringRuntime {
                sq_entries: *sq_entries,
                cq_entries: *cq_entries,
                buf_ring_size: *buf_ring_size,
                buf_size: *buf_size,
            },
        ),
        AsyncRuntimeConfig::Epoll { max_events } => EpollRuntime {
            max_events: *max_events,
        }
        .into(),
        AsyncRuntimeConfig::Kqueue { max_events } => KqueueRuntime {
            max_events: *max_events,
        }
        .into(),
        AsyncRuntimeConfig::IoUring {
            sq_entries,
            cq_entries,
            buf_ring_size,
            buf_size,
        } => IoUringRuntime {
            sq_entries: *sq_entries,
            cq_entries: *cq_entries,
            buf_ring_size: *buf_ring_size,
            buf_size: *buf_size,
        }
        .into(),
    }
}

impl From<EpollRuntime> for SelectedRuntime {
    fn from(runtime: EpollRuntime) -> Self {
        Self::Epoll(runtime)
    }
}

impl From<KqueueRuntime> for SelectedRuntime {
    fn from(runtime: KqueueRuntime) -> Self {
        Self::Kqueue(runtime)
    }
}

impl From<IoUringRuntime> for SelectedRuntime {
    fn from(runtime: IoUringRuntime) -> Self {
        Self::IoUring(runtime)
    }
}

#[cfg(target_os = "linux")]
fn select_auto_runtime(max_events: usize, io_uring: IoUringRuntime) -> SelectedRuntime {
    match io_uring.probe() {
        Ok(()) => {
            eprintln!("selected runtime: io_uring (configured: auto)");
            SelectedRuntime::IoUring(io_uring)
        }
        Err(error) => {
            eprintln!("selected runtime: epoll (configured: auto; io_uring unavailable: {error})");
            SelectedRuntime::Epoll(EpollRuntime { max_events })
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn select_auto_runtime(max_events: usize, _io_uring: IoUringRuntime) -> SelectedRuntime {
    eprintln!("selected runtime: kqueue (configured: auto; platform is not Linux)");
    SelectedRuntime::Kqueue(KqueueRuntime { max_events })
}

fn install_shutdown_signals(shutdown: &ShutdownHandle) -> io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])?;
    let shutdown = shutdown.clone();
    std::thread::spawn(move || {
        if signals.forever().next().is_some() {
            shutdown.request();
        }
    });
    Ok(())
}

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu_id: usize) -> io::Result<()> {
    let core_ids = core_affinity::get_core_ids()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "CPU affinity is unavailable"))?;
    let core = core_ids
        .get(cpu_id % core_ids.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no CPU cores are available"))?;

    if core_affinity::set_for_current(*core) {
        Ok(())
    } else {
        Err(io::Error::other("failed to set worker CPU affinity"))
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(cpu_id: usize) -> io::Result<()> {
    eprintln!(
        "[Worker {}] CPU affinity is unavailable on this platform; using the OS scheduler.",
        cpu_id
    );
    Ok(())
}
