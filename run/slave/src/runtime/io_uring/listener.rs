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
    accept_mode: AcceptMode,
    accept_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AcceptMode {
    SingleShot,
    Multishot,
}

impl UringListener {
    pub(super) fn new(
        socket: OwnedFd,
        default_server: usize,
        server_indices: Vec<usize>,
        accept_mode: AcceptMode,
    ) -> Self {
        Self {
            socket,
            default_server,
            server_indices,
            generation: 1,
            accept_mode,
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

        let entry = match self.accept_mode {
            AcceptMode::SingleShot => opcode::Accept::new(
                types::Fd(self.socket.as_raw_fd()),
                ptr::null_mut(),
                ptr::null_mut(),
            )
            .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
            .build(),
            AcceptMode::Multishot => opcode::AcceptMulti::new(types::Fd(self.socket.as_raw_fd()))
                .flags(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)
                .build(),
        };
        Ok(entry.user_data(operation.encode()))
    }

    pub(super) const fn accept_pending(&self) -> bool {
        self.accept_pending
    }

    pub(super) fn mark_accept_submitted(&mut self) {
        self.accept_pending = true;
    }

    pub(super) const fn accept_mode(&self) -> AcceptMode {
        self.accept_mode
    }

    pub(super) fn record_completion(&mut self, has_more: bool) {
        self.accept_pending = self.accept_mode == AcceptMode::Multishot && has_more;
    }

    pub(super) fn fall_back_to_single_shot(&mut self) {
        self.accept_mode = AcceptMode::SingleShot;
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
