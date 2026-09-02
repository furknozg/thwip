#![cfg(unix)]

use proxy_common::{Action, Location, PathMatcher, Server};
#[cfg(target_os = "linux")]
use slave::EpollRuntime;
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
use slave::KqueueRuntime;
use slave::{BoundListenerGroup, Runtime, ShutdownHandle, WorkerContext, WorkerLimits};
use socket2::Socket;
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    thread::{self, JoinHandle},
    time::Duration,
};

struct TestWorker {
    address: SocketAddr,
    shutdown: ShutdownHandle,
    thread: Option<JoinHandle<std::io::Result<()>>>,
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        self.shutdown.request();
        if let Some(worker) = self.thread.take() {
            worker
                .join()
                .expect("worker thread should not panic")
                .expect("worker should shut down cleanly");
        }
    }
}

fn server(name: &str, body: &str, listen: SocketAddr) -> Server {
    Server {
        server_name: Some(name.into()),
        listen,
        locations: vec![Location {
            matcher: PathMatcher::Prefix { path: "/".into() },
            action: Action::Response {
                status: 200,
                body: body.into(),
            },
        }],
    }
}

fn request(address: SocketAddr, host: &str) -> String {
    let mut stream = TcpStream::connect(address).expect("connect to worker");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
    write!(stream, "GET / HTTP/1.1\r\nHost: {host}\r\n\r\n").expect("write request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

fn run_readiness(context: WorkerContext) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        EpollRuntime { max_events: 64 }.run(context)
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    {
        KqueueRuntime { max_events: 64 }.run(context)
    }
}

fn start_worker(hosts: &[(&str, String)], limits: WorkerLimits) -> TestWorker {
    let requested_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = TcpListener::bind(requested_address).expect("bind listener");
    socket
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    let address = socket.local_addr().expect("read bound address");
    let shutdown = ShutdownHandle::new();
    let servers: Vec<Server> = hosts
        .iter()
        .map(|(name, body)| server(name, body, address))
        .collect();
    let context = WorkerContext {
        listener_groups: vec![BoundListenerGroup {
            socket,
            address,
            default_server: 0,
            server_indices: (0..servers.len()).collect(),
        }],
        servers,
        shutdown: shutdown.clone(),
        limits,
    };
    let worker = thread::spawn(move || run_readiness(context));

    TestWorker {
        address,
        shutdown,
        thread: Some(worker),
    }
}

#[test]
fn routes_shared_listener_by_host_and_uses_its_default_server() {
    let worker = start_worker(
        &[("one.test", "one".into()), ("two.test", "two".into())],
        WorkerLimits::default(),
    );

    assert!(request(worker.address, "one.test").ends_with("\r\n\r\none"));
    assert!(request(worker.address, "TWO.TEST:8080").ends_with("\r\n\r\ntwo"));
    assert!(request(worker.address, "unknown.test").ends_with("\r\n\r\none"));
}

#[test]
fn waits_for_fragmented_request_head_and_body() {
    let worker = start_worker(&[("body.test", "complete".into())], WorkerLimits::default());
    let mut stream = TcpStream::connect(worker.address).expect("connect to worker");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    stream.write_all(b"POST / HT").unwrap();
    stream
        .write_all(b"TP/1.1\r\nHost: body.test\r\nContent-Length: 5\r\n\r\nhe")
        .unwrap();

    let mut byte = [0; 1];
    let error = stream
        .read(&mut byte)
        .expect_err("worker must wait for the complete request body");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));

    stream.write_all(b"llo").unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.ends_with("\r\n\r\ncomplete"));
    assert!(response.contains("Connection: close\r\n"));
}

#[test]
fn completes_large_responses_for_slow_readers() {
    let body = "x".repeat(256 * 1024);
    let worker = start_worker(&[("slow.test", body.clone())], WorkerLimits::default());
    let mut stream = TcpStream::connect(worker.address).expect("connect to worker");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: slow.test\r\n\r\n")
        .unwrap();

    thread::sleep(Duration::from_millis(25));
    let mut response = Vec::new();
    let mut chunk = [0_u8; 257];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                response.extend_from_slice(&chunk[..read]);
                thread::sleep(Duration::from_micros(100));
            }
            Err(error) => panic!("slow response read failed: {error}"),
        }
    }

    assert!(response.ends_with(body.as_bytes()));
}

#[test]
fn client_disconnect_does_not_stop_the_worker() {
    let worker = start_worker(&[("live.test", "alive".into())], WorkerLimits::default());
    {
        let mut disconnected = TcpStream::connect(worker.address).unwrap();
        disconnected.write_all(b"GET / HTTP/1.1\r\nHost:").unwrap();
    }

    thread::sleep(Duration::from_millis(25));
    assert!(request(worker.address, "live.test").ends_with("\r\n\r\nalive"));
}

#[test]
fn half_closed_client_can_receive_a_queued_response() {
    let worker = start_worker(&[("half.test", "complete".into())], WorkerLimits::default());
    let mut stream = TcpStream::connect(worker.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: half.test\r\n\r\n")
        .unwrap();
    stream.shutdown(Shutdown::Write).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.ends_with("\r\n\r\ncomplete"));
}

#[test]
fn resets_during_reads_and_writes_do_not_stop_the_worker() {
    let large = "x".repeat(512 * 1024);
    let worker = start_worker(
        &[("large.test", large), ("live.test", "alive".into())],
        WorkerLimits::default(),
    );

    let mut read_reset = TcpStream::connect(worker.address).unwrap();
    read_reset.write_all(b"GET / HTTP/1.1\r\nHost:").unwrap();
    let read_reset = Socket::from(read_reset);
    read_reset.set_linger(Some(Duration::ZERO)).unwrap();
    drop(read_reset);

    let mut write_reset = TcpStream::connect(worker.address).unwrap();
    write_reset
        .write_all(b"GET / HTTP/1.1\r\nHost: large.test\r\n\r\n")
        .unwrap();
    let write_reset = Socket::from(write_reset);
    write_reset.set_linger(Some(Duration::ZERO)).unwrap();
    drop(write_reset);

    thread::sleep(Duration::from_millis(50));
    assert!(request(worker.address, "live.test").ends_with("\r\n\r\nalive"));
}

#[test]
fn rejects_unsupported_transfer_encoding_and_invalid_content_length() {
    let worker = start_worker(&[("body.test", "unused".into())], WorkerLimits::default());

    let mut chunked = TcpStream::connect(worker.address).unwrap();
    chunked
        .write_all(b"POST / HTTP/1.1\r\nHost: body.test\r\nTransfer-Encoding: chunked\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    chunked.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 501 Not Implemented\r\n"));

    let mut invalid = TcpStream::connect(worker.address).unwrap();
    invalid
        .write_all(b"POST / HTTP/1.1\r\nHost: body.test\r\nContent-Length: nope\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    invalid.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
}
