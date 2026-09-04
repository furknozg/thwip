use super::connection::ConnectionId;
use std::{
    io,
    net::{SocketAddr, ToSocketAddrs},
    os::fd::{AsRawFd, OwnedFd},
    sync::{mpsc, Arc, Mutex},
    thread,
};

struct ResolveTask {
    connection_id: ConnectionId,
    address: String,
}

pub(super) struct ResolveResult {
    pub(super) connection_id: ConnectionId,
    pub(super) addresses: io::Result<Vec<SocketAddr>>,
}

pub(super) struct DnsResolver {
    tasks: mpsc::Sender<ResolveTask>,
    results: mpsc::Receiver<ResolveResult>,
}

impl DnsResolver {
    pub(super) fn new(thread_count: usize, eventfd: Arc<OwnedFd>) -> io::Result<Self> {
        let (task_sender, task_receiver) = mpsc::channel::<ResolveTask>();
        let (result_sender, result_receiver) = mpsc::channel();
        let task_receiver = Arc::new(Mutex::new(task_receiver));

        for index in 0..thread_count {
            let tasks = Arc::clone(&task_receiver);
            let results = result_sender.clone();
            let eventfd = Arc::clone(&eventfd);
            thread::Builder::new()
                .name(format!("thwip-uring-dns-{index}"))
                .spawn(move || loop {
                    let task = match tasks.lock() {
                        Ok(receiver) => receiver.recv(),
                        Err(_) => return,
                    };
                    let Ok(task) = task else { return };
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
                    let value = 1_u64;
                    unsafe {
                        libc::write(
                            eventfd.as_raw_fd(),
                            (&value as *const u64).cast(),
                            std::mem::size_of::<u64>(),
                        );
                    }
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
