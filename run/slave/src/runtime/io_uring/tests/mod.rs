use super::{
    connection::next_generation,
    listener::{AcceptMode, UringListener},
    operation::{OperationId, OperationKind, CONTROL_USER_DATA},
    worker::multishot_is_unsupported,
};
use std::{os::fd::OwnedFd, os::unix::net::UnixStream};

#[test]
fn operation_ids_round_trip_every_socket_operation() {
    for operation in [
        OperationId::accept(7, 9),
        OperationId::read(7, 9),
        OperationId::write(7, 9),
        OperationId::proxy_connect(7, 9),
        OperationId::proxy_write(7, 9),
        OperationId::proxy_read(7, 9),
    ] {
        assert_eq!(OperationId::decode(operation.encode()), Some(operation));
    }
}

#[test]
fn operation_ids_reject_control_unknown_and_zero_generation_values() {
    assert_eq!(OperationId::decode(CONTROL_USER_DATA), None);
    assert_eq!(OperationId::decode(99_u64 << 48), None);
    assert_eq!(
        OperationId::decode(
            OperationId {
                slot: 1,
                generation: 0,
                kind: OperationKind::Read,
            }
            .encode()
        ),
        None
    );
}

#[test]
fn connection_generations_wrap_without_using_zero() {
    assert_eq!(next_generation(0), 1);
    assert_eq!(next_generation(u16::MAX), 1);
}

fn listener(mode: AcceptMode) -> UringListener {
    let (socket, _peer) = UnixStream::pair().unwrap();
    UringListener::new(OwnedFd::from(socket), 0, vec![0], None, mode)
}

#[test]
fn multishot_accept_stays_pending_only_while_cqe_has_more() {
    let mut listener = listener(AcceptMode::Multishot);
    listener.mark_accept_submitted();
    listener.record_completion(true);
    assert!(listener.accept_pending());

    listener.record_completion(false);
    assert!(!listener.accept_pending());
}

#[test]
fn single_shot_accept_always_becomes_ready_for_resubmission() {
    let mut listener = listener(AcceptMode::SingleShot);
    listener.mark_accept_submitted();
    listener.record_completion(true);
    assert!(!listener.accept_pending());
}

#[test]
fn unsupported_multishot_errors_trigger_single_shot_fallback() {
    assert!(multishot_is_unsupported(-libc::EINVAL));
    assert!(multishot_is_unsupported(-libc::EOPNOTSUPP));
    assert!(!multishot_is_unsupported(-libc::ECONNABORTED));

    let mut listener = listener(AcceptMode::Multishot);
    listener.mark_accept_submitted();
    listener.fall_back_to_single_shot();
    assert_eq!(listener.accept_mode(), AcceptMode::SingleShot);
    assert!(!listener.accept_pending());
}
