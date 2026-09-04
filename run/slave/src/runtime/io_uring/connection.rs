#![allow(dead_code)] // Fields become active when Recv submission is added.

use crate::RequestHead;
use std::os::fd::OwnedFd;

pub(super) struct UringConnection {
    pub(super) socket: OwnedFd,
    pub(super) generation: u16,
    pub(super) listener_index: usize,
    pub(super) read_buffer: Box<[u8]>,
    pub(super) read_pending: bool,
    pub(super) request_buffer: Vec<u8>,
    pub(super) pending_request: Option<PendingRequest>,
    pub(super) request: Option<CompletedRequest>,
}

pub(super) struct PendingRequest {
    pub(super) head: RequestHead,
    pub(super) body_start: usize,
    pub(super) body_end: usize,
}

pub(super) struct CompletedRequest {
    pub(super) head: RequestHead,
    pub(super) body: Vec<u8>,
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
            request_buffer: Vec::with_capacity(buffer_size),
            pending_request: None,
            request: None,
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

    pub(super) fn mark_read_completed(&mut self) {
        self.read_pending = false;
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
