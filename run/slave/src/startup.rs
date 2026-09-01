use proxy_common::{AsyncRuntimeConfig, Config};
use std::io;

use crate::{
    bind_worker_listeners, EpollRuntime, IoUringRuntime, KqueueRuntime, Runtime, ShutdownHandle,
    WorkerContext,
};

pub fn start_worker(cpu_id: usize, config: &Config) -> io::Result<()> {
    let shutdown = ShutdownHandle::new();
    install_shutdown_signals(&shutdown)?;
    pin_to_cpu(cpu_id)?;
    let listener_groups = bind_worker_listeners(config)?;
    let context = WorkerContext {
        listener_groups,
        servers: config.http.servers.clone(),
        shutdown,
    };

    match &config.runtime {
        AsyncRuntimeConfig::Epoll { max_events } => EpollRuntime {
            max_events: *max_events,
        }
        .run(context),
        AsyncRuntimeConfig::Kqueue { max_events } => KqueueRuntime {
            max_events: *max_events,
        }
        .run(context),
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
        .run(context),
    }
}

fn install_shutdown_signals(shutdown: &ShutdownHandle) -> io::Result<()> {
    for signal in [
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ] {
        signal_hook::flag::register(signal, shutdown.flag())?;
    }
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
