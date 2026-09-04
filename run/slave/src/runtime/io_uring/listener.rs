use std::{
    io,
    os::fd::{AsRawFd, OwnedFd},
    ptr,
};

use io_uring::{opcode, squeue, types};

use super::operation::OperationId;

pub(super) struct UringListener {
    socket: OwnedFd,
    #[allow(dead_code)] // Used when accepted connections begin HTTP routing.
    pub(super) default_server: usize,
    #[allow(dead_code)] // Used when accepted connections begin HTTP routing.
    pub(super) server_indices: Vec<usize>,
    generation: u16,
    accept_pending: bool,
}

impl UringListener {
    pub(super) fn new(socket: OwnedFd, default_server: usize, server_indices: Vec<usize>) -> Self {
        Self {
            socket,
            default_server,
            server_indices,
            generation: 1,
            accept_pending: false,
        }
    }

    /// Build one accept submission for this listener. The listener continues
    /// to own the descriptor for the entire lifetime of the operation.
    pub(super) fn accept_entry(&self, listener_index: usize) -> io::Result<squeue::Entry> {
        let slot = u32::try_from(listener_index).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "listener index exceeds io_uring operation capacity",
            )
        })?;
        let operation = OperationId::accept(slot, self.generation);

        Ok(opcode::Accept::new(
            types::Fd(self.socket.as_raw_fd()),
            ptr::null_mut(),
            ptr::null_mut(),
        )
        .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
        .build()
        .user_data(operation.encode()))
    }

    pub(super) const fn accept_pending(&self) -> bool {
        self.accept_pending
    }

    pub(super) fn mark_accept_submitted(&mut self) {
        self.accept_pending = true;
    }

    pub(super) fn mark_accept_completed(&mut self) {
        self.accept_pending = false;
    }

    pub(super) fn matches_generation(&self, generation: u16) -> bool {
        self.generation == generation
    }

    pub(super) fn accept_operation(&self, listener_index: usize) -> io::Result<OperationId> {
        let slot = u32::try_from(listener_index).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "listener index exceeds io_uring operation capacity",
            )
        })?;
        Ok(OperationId::accept(slot, self.generation))
    }
}
