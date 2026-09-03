#![allow(dead_code)] // Scaffolding until the first SQE/CQE dispatch milestone.

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperationKind {
    Accept = 1,
    Read = 2,
    Write = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OperationId {
    pub(super) slot: u32,
    pub(super) generation: u16,
    pub(super) kind: OperationKind,
}

impl OperationId {
    /// Encode this operation into the `user_data` field carried by an SQE/CQE.
    pub fn encode(self) -> u64 {
        todo!("encode the operation kind, generation, and slot")
    }

    /// Decode completion `user_data`, rejecting reserved or unknown values.
    pub fn decode(_value: u64) -> Option<Self> {
        todo!("decode and validate an io_uring operation identifier")
    }
}
