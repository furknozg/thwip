use super::{Runtime, WorkerContext};
use std::io;

pub struct KqueueRuntime {
    pub max_events: usize,
}

impl Runtime for KqueueRuntime {
    fn run(self, context: WorkerContext) -> io::Result<()> {
        #[cfg(any(
            target_os = "macos",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly"
        ))]
        {
            super::readiness::run(context, self.max_events)
        }
        #[cfg(not(any(
            target_os = "macos",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly"
        )))]
        {
            let _ = context;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kqueue is only supported on macOS and BSD",
            ))
        }
    }
}
