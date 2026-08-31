use proxy_common::{AsyncRuntimeConfig, Config};
use std::{io, net::TcpListener};

use crate::{bind_worker_listeners, run_epoll};

pub fn start_worker(cpu_id: usize, config: &Config) -> io::Result<()> {
    pin_to_cpu(cpu_id)?;
    let listeners = bind_worker_listeners(config)?;

    match &config.runtime {
        AsyncRuntimeConfig::Epoll { max_events } => run_epoll(listeners, *max_events),
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
    listeners: Vec<TcpListener>,
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
    _listeners: Vec<TcpListener>,
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
