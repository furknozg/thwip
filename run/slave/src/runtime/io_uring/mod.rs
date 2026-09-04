use super::{Runtime, WorkerContext};
use std::io;

#[derive(Debug, Clone, Copy)]
pub struct IoUringRuntime {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_ring_size: u32,
    pub buf_size: usize,
}

impl IoUringRuntime {
    /// Verify that this host can construct every resource required by the
    /// direct driver without taking ownership of worker listeners.
    pub fn probe(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use io_uring::{cqueue, squeue, IoUring};

            // Field order is intentional: the kernel ring must be destroyed
            // before its registered buffer memory is released.
            struct ProbeResources {
                _ring: IoUring<squeue::Entry, cqueue::Entry>,
                _buffers: buffer_ring::ProvidedBufferRing,
            }

            let ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
                .setup_cqsize(self.cq_entries)
                .build(self.sq_entries)?;
            let capabilities = capabilities::Capabilities::probe(&ring)?;
            capabilities.validate_required()?;
            let configured_buffer_size = u32::try_from(self.buf_size).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring buf_size is too large",
                )
            })?;
            if configured_buffer_size == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "io_uring buf_size must be greater than zero",
                ));
            }
            // Registration validates the kernel feature and configured ring
            // depth. The preflight uses minimal backing buffers so `auto`
            // does not temporarily allocate the worker's complete data pool.
            let buffers = buffer_ring::ProvidedBufferRing::register(&ring, self.buf_ring_size, 1)?;
            drop(ProbeResources {
                _ring: ring,
                _buffers: buffers,
            });
            Ok(())
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
mod capabilities;

#[cfg(target_os = "linux")]
mod worker;

#[cfg(target_os = "linux")]
mod listener;

#[cfg(target_os = "linux")]
mod resolver;

#[cfg(all(test, target_os = "linux"))]
mod tests;
