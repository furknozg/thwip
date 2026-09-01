use proxy_common::{AsyncRuntimeConfig, Config};
use std::io;

use crate::{bind_worker_listeners, run_epoll_with_shutdown, BoundListener, ShutdownHandle};

pub fn start_worker(cpu_id: usize, config: &Config) -> io::Result<()> {
    let shutdown = ShutdownHandle::new();
    install_shutdown_signals(&shutdown)?;
    pin_to_cpu(cpu_id)?;
    let listeners = bind_worker_listeners(config)?;

    match &config.runtime {
        AsyncRuntimeConfig::Epoll { max_events } => run_epoll_with_shutdown(
            listeners,
            config.http.servers.clone(),
            *max_events,
            shutdown,
        ),
        AsyncRuntimeConfig::IoUring {
            sq_entries,
            cq_entries,
            buf_ring_size,
            buf_size,
        } => run_io_uring(
            listeners,
            *sq_entries,
            *cq_entries,
            *buf_ring_size,
            *buf_size,
        ),
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

#[cfg(target_os = "linux")]
pub fn run_io_uring(
    listeners: Vec<BoundListener>,
    sq_entries: u32,
    cq_entries: u32,
    buf_ring_size: u32,
    buf_size: usize,
) -> io::Result<()> {
    let _ = (listeners, sq_entries, cq_entries, buf_ring_size, buf_size);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "io_uring runtime has not been implemented yet",
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn run_io_uring(
    _listeners: Vec<BoundListener>,
    _sq_entries: u32,
    _cq_entries: u32,
    _buf_ring_size: u32,
    _buf_size: usize,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "io_uring is only supported on Linux",
    ))
}
