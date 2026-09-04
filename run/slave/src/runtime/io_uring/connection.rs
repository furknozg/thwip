#![allow(dead_code)] // Fields become active when Recv submission is added.

use std::os::fd::OwnedFd;

pub(super) struct UringConnection {
    pub(super) socket: OwnedFd,
    pub(super) generation: u16,
    pub(super) listener_index: usize,
    pub(super) read_buffer: Box<[u8]>,
    pub(super) read_pending: bool,
    pub(super) received_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConnectionId {
    pub(super) slot: u32,
    pub(super) generation: u16,
}

impl UringConnection {
    pub(super) fn new(
        socket: OwnedFd,
        generation: u16,
        listener_index: usize,
        buffer_size: usize,
    ) -> Self {
        Self {
            socket,
            generation,
            listener_index,
            read_buffer: vec![0; buffer_size].into_boxed_slice(),
            read_pending: false,
            received_len: 0,
        }
    }

    pub(super) fn id(&self, slot: u32) -> ConnectionId {
        ConnectionId {
            slot,
            generation: self.generation,
        }
    }

    pub(super) fn matches_generation(&self, generation: u16) -> bool {
        self.generation == generation
    }

    pub(super) fn mark_read_submitted(&mut self) {
        self.read_pending = true;
    }

    pub(super) fn mark_read_completed(&mut self, received_len: usize) {
        self.read_pending = false;
        self.received_len = received_len;
    }
}

pub(super) fn next_generation(previous: u16) -> u16 {
    let next = previous.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}
