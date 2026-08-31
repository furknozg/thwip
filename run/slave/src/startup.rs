use proxy_common::{AsyncRuntimeConfig, Config};
use std::{io, net::TcpListener};

use crate::bind_worker_listeners;

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
pub fn run_epoll(listeners: Vec<std::net::TcpListener>, max_events: usize) -> std::io::Result<()> {
    use mio::{net::TcpListener, Events, Interest, Poll, Token};
    use std::io::ErrorKind;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(max_events.max(1));

    // `bind_worker_listener` already made these sockets nonblocking.
    let mut listeners: Vec<TcpListener> =
        listeners.into_iter().map(TcpListener::from_std).collect();

    // Token(0..N) identifies the corresponding listening socket.
    for (index, listener) in listeners.iter_mut().enumerate() {
        poll.registry()
            .register(listener, Token(index), Interest::READABLE)?;
    }

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            if !event.is_readable() {
                continue;
            }

            let listener_index = event.token().0;
            let listener = &mut listeners[listener_index];

            // Important: drain accepts until WouldBlock.
            loop {
                match listener.accept() {
                    Ok((stream, peer_address)) => {
                        println!("accepted connection from {peer_address}");

                        // Temporary first milestone: close it immediately.
                        // Dropping `stream` closes the connection.
                        drop(stream);
                    }

                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        break;
                    }

                    Err(error) => {
                        eprintln!("accept failed on listener {}: {}", listener_index, error);
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn run_epoll(
    _listeners: Vec<std::net::TcpListener>,
    _max_events: usize,
) -> std::io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "epoll is only supported on Linux",
    ))
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
