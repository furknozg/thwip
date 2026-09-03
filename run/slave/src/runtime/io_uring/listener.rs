use std::{io, os::fd::OwnedFd};

use io_uring::squeue;

use crate::runtime::io_uring::operation::OperationId;

pub(super) struct UringListener {
    pub socket: OwnedFd,
    pub default_server: usize,
    pub server_indices: Vec<usize>,
    pub generation: u16,
    pub accept_pending: bool,
}

impl UringListener {
}

