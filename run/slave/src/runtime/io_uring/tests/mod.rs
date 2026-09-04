use super::{
    connection::next_generation,
    operation::{OperationId, OperationKind, CONTROL_USER_DATA},
};

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
