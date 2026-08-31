use slave::bind_worker_listener;
use std::io;

#[cfg(target_os = "linux")]
use slave::DEFAULT_BACKLOG;

#[test]
fn rejects_a_nonpositive_backlog() {
    let error = bind_worker_listener("127.0.0.1:0".parse().unwrap(), 0).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[cfg(target_os = "linux")]
#[test]
fn binds_a_nonblocking_loopback_listener() {
    let listener = bind_worker_listener("127.0.0.1:0".parse().unwrap(), DEFAULT_BACKLOG).unwrap();

    assert_ne!(listener.local_addr().unwrap().port(), 0);

    let error = listener.accept().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}
