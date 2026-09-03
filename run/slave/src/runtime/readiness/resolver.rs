#![cfg(unix)]

use mio::Waker;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    sync::{mpsc, Arc, Mutex},
    thread,
};

use super::token::ConnectionId;

struct ResolveTask {
    connection_id: ConnectionId,
    address: String,
}

pub(super) struct ResolveResult {
    pub(super) connection_id: ConnectionId,
    pub(super) addresses: io::Result<Vec<SocketAddr>>,
}

/// A small per-worker pool keeps blocking system DNS calls away from the
/// readiness thread. Results wake the poller and retain the connection
/// generation that requested them, so late answers are harmless.
pub(super) struct DnsResolver {
    tasks: mpsc::Sender<ResolveTask>,
    results: mpsc::Receiver<ResolveResult>,
}

impl DnsResolver {
    pub(super) fn new(thread_count: usize, waker: Arc<Waker>) -> io::Result<Self> {
        let (task_sender, task_receiver) = mpsc::channel::<ResolveTask>();
        let (result_sender, result_receiver) = mpsc::channel();
        let task_receiver = Arc::new(Mutex::new(task_receiver));

        for index in 0..thread_count {
            let tasks = Arc::clone(&task_receiver);
            let results = result_sender.clone();
            let waker = Arc::clone(&waker);
            thread::Builder::new()
                .name(format!("thwip-dns-{index}"))
                .spawn(move || loop {
                    let task = match tasks.lock() {
                        Ok(receiver) => receiver.recv(),
                        Err(_) => return,
                    };
                    let Ok(task) = task else {
                        return;
                    };
                    let addresses = task.address.to_socket_addrs().map(Iterator::collect);
                    if results
                        .send(ResolveResult {
                            connection_id: task.connection_id,
                            addresses,
                        })
                        .is_err()
                    {
                        return;
                    }
                    let _ = waker.wake();
                })?;
        }

        Ok(Self {
            tasks: task_sender,
            results: result_receiver,
        })
    }

    pub(super) fn resolve(&self, connection_id: ConnectionId, address: String) -> io::Result<()> {
        self.tasks
            .send(ResolveTask {
                connection_id,
                address,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "DNS resolver stopped"))
    }

    pub(super) fn drain(&self) -> Vec<ResolveResult> {
        self.results.try_iter().collect()
    }
}
