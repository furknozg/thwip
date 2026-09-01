use super::{Runtime, WorkerContext};
use std::io;

pub struct IoUringRuntime {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_ring_size: u32,
    pub buf_size: usize,
}

impl Runtime for IoUringRuntime {
    fn run(self, _context: WorkerContext) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let _ = (
                self.sq_entries,
                self.cq_entries,
                self.buf_ring_size,
                self.buf_size,
            );
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring runtime has not been implemented yet",
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is only supported on Linux",
            ))
        }
    }
}
