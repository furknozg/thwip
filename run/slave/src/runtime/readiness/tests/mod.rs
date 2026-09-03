use std::time::Duration;

use super::{
    proxy::ProxyPhase,
    token::{next_generation, ConnectionId, SocketRole},
};
use crate::ProxyLimits;

#[test]
fn each_proxy_phase_uses_its_own_timeout() {
    let limits = ProxyLimits {
        connect_timeout: Duration::from_millis(11),
        write_timeout: Duration::from_millis(22),
        read_timeout: Duration::from_millis(33),
    };

    assert_eq!(
        ProxyPhase::Connecting.timeout(limits),
        Duration::from_millis(11)
    );
    assert_eq!(
        ProxyPhase::WritingRequest.timeout(limits),
        Duration::from_millis(22)
    );
    assert_eq!(
        ProxyPhase::ReadingResponse.timeout(limits),
        Duration::from_millis(33)
    );
}

#[test]
fn tokens_round_trip_slot_generation_and_role() {
    let id = ConnectionId {
        slot: 42,
        generation: 7,
    };
    assert_eq!(
        ConnectionId::from_token(id.token(SocketRole::Client).unwrap()),
        Some((id, SocketRole::Client))
    );
    assert_eq!(
        ConnectionId::from_token(id.token(SocketRole::Upstream).unwrap()),
        Some((id, SocketRole::Upstream))
    );
}

#[test]
fn reused_slots_receive_distinct_tokens() {
    let old = ConnectionId {
        slot: 3,
        generation: 1,
    };
    let replacement = ConnectionId {
        slot: 3,
        generation: next_generation(old.generation),
    };

    assert_ne!(
        old.token(SocketRole::Client).unwrap(),
        replacement.token(SocketRole::Client).unwrap()
    );
    assert_eq!(
        ConnectionId::from_token(replacement.token(SocketRole::Upstream).unwrap()),
        Some((replacement, SocketRole::Upstream))
    );
}
