use super::{Runtime, WorkerContext};
use std::io;

pub struct IoUringRuntime {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_ring_size: u32,
    pub buf_size: usize,
}

impl Runtime for IoUringRuntime {
    fn run(self, context: WorkerContext) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            worker::IoUringWorker::new(self, context)?.run()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = context;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is only supported on Linux",
            ))
        }
    }
}

#[cfg(target_os = "linux")]
mod operation;

#[cfg(target_os = "linux")]
mod connection;

#[cfg(target_os = "linux")]
mod buffer_ring;

#[cfg(target_os = "linux")]
mod worker;

#[cfg(target_os = "linux")]
mod listener;

#[cfg(target_os = "linux")]
mod resolver;

#[cfg(all(test, target_os = "linux"))]
mod tests;
