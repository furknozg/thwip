#![cfg(unix)]

use proxy_common::{Action, BalancePolicy, Location, PathMatcher, Server, UpstreamEndpoint};
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
use slave::{
    BoundListenerGroup, DnsLimits, ProxyLimits, Runtime, ShutdownHandle, WorkerContext,
    WorkerLimits,
};
use socket2::Socket;
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
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

fn server_with_action(name: &str, action: Action, listen: SocketAddr) -> Server {
    Server {
        server_name: Some(name.into()),
        listen,
        ssl: None,
        locations: vec![Location {
            matcher: PathMatcher::Prefix { path: "/".into() },
            action,
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
    let actions: Vec<(&str, Action)> = hosts
        .iter()
        .map(|(name, body)| {
            (
                *name,
                Action::Response {
                    status: 200,
                    body: body.clone(),
                },
            )
        })
        .collect();
    start_worker_with_actions(&actions, limits)
}

fn start_worker_with_actions(hosts: &[(&str, Action)], limits: WorkerLimits) -> TestWorker {
    start_worker_with_proxy_limits(hosts, limits, ProxyLimits::default())
}

fn start_worker_with_proxy_limits(
    hosts: &[(&str, Action)],
    limits: WorkerLimits,
    proxy_limits: ProxyLimits,
) -> TestWorker {
    let requested_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = TcpListener::bind(requested_address).expect("bind listener");
    socket
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    let address = socket.local_addr().expect("read bound address");
    let shutdown = ShutdownHandle::new();
    let servers: Vec<Server> = hosts
        .iter()
        .map(|(name, action)| server_with_action(name, action.clone(), address))
        .collect();
    let context = WorkerContext {
        listener_groups: vec![BoundListenerGroup {
            socket,
            address,
            default_server: 0,
            server_indices: (0..servers.len()).collect(),
        }],
        ssl_configs: vec![None; servers.len()],
        servers,
        shutdown: shutdown.clone(),
        limits,
        proxy_limits,
        dns_limits: DnsLimits::default(),
        metrics: slave::WorkerMetrics::default(),
        upstream_groups: Default::default(),
    };
    let worker = thread::spawn(move || run_readiness(context));

    TestWorker {
        address,
        shutdown,
        thread: Some(worker),
    }
}

#[test]
fn streams_static_files_larger_than_the_in_memory_limit() {
    let root = std::env::temp_dir().join(format!("thwip-large-static-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let body = vec![b'z'; 5 * 1024 * 1024];
    std::fs::write(root.join("index.html"), &body).unwrap();
    let worker = start_worker_with_actions(
        &[(
            "static.test",
            Action::Static {
                directory: root.to_string_lossy().into_owned(),
            },
        )],
        WorkerLimits::default(),
    );

    let mut client = TcpStream::connect(worker.address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: static.test\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let body_start = response
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap()
        + 4;
    assert_eq!(&response[body_start..], body);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn streams_request_body_to_upstream_and_response_back_to_client() {
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let (request_sender, request_receiver) = std::sync::mpsc::channel();
    let upstream_thread = thread::spawn(move || {
        let (mut stream, _) = upstream.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.ends_with(b"ping") {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
        }
        request_sender.send(request).unwrap();

        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nstre")
            .unwrap();
        thread::sleep(Duration::from_millis(20));
        stream.write_all(b"amed").unwrap();
    });
    let worker = start_worker_with_actions(
        &[(
            "proxy.test",
            Action::Proxy {
                upstream: Some(format!("http://{upstream_address}")),
                upstream_group: None,
                upstreams: Vec::new(),
                policy: BalancePolicy::default(),
            },
        )],
        WorkerLimits::default(),
    );

    let mut client = TcpStream::connect(worker.address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client
        .write_all(b"POST /upload HTTP/1.1\r\nHost: proxy.test\r\nContent-Length: 4\r\n\r\nping")
        .unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();

    assert!(response.ends_with(b"streamed"));
    let forwarded = String::from_utf8(request_receiver.recv().unwrap()).unwrap();
    assert!(forwarded.starts_with("POST /upload HTTP/1.1\r\n"));
    assert!(forwarded.contains(&format!("Host: {upstream_address}\r\n")));
    assert!(forwarded.contains("Connection: close\r\n"));
    assert!(forwarded.ends_with("\r\n\r\nping"));
    upstream_thread.join().unwrap();
}

#[test]
fn balances_proxy_requests_across_an_upstream_group() {
    let first = TcpListener::bind("127.0.0.1:0").unwrap();
    let second = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoints = [first.local_addr().unwrap(), second.local_addr().unwrap()];
    let backends: Vec<_> = [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buffer).unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let body = format!("backend-{index}");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            })
        })
        .collect();
    let worker = start_worker_with_actions(
        &[(
            "balanced.test",
            Action::Proxy {
                upstream: None,
                upstream_group: None,
                upstreams: endpoints
                    .iter()
                    .map(|address| UpstreamEndpoint {
                        url: format!("http://{address}"),
                        weight: 1,
                    })
                    .collect(),
                policy: BalancePolicy::RoundRobin,
            },
        )],
        WorkerLimits::default(),
    );

    assert!(request(worker.address, "balanced.test").ends_with("backend-0"));
    assert!(request(worker.address, "balanced.test").ends_with("backend-1"));
    for backend in backends {
        backend.join().unwrap();
    }
}

#[test]
fn returns_bad_gateway_when_upstream_connect_fails() {
    let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable_address = unavailable.local_addr().unwrap();
    drop(unavailable);
    let worker = start_worker_with_actions(
        &[(
            "proxy.test",
            Action::Proxy {
                upstream: Some(format!("http://{unavailable_address}")),
                upstream_group: None,
                upstreams: Vec::new(),
                policy: BalancePolicy::default(),
            },
        )],
        WorkerLimits::default(),
    );

    let response = request(worker.address, "proxy.test");
    assert!(
        response.starts_with("HTTP/1.1 502 Bad Gateway\r\n"),
        "unexpected proxy failure response: {response:?}"
    );
}

#[test]
fn resolves_hostname_upstreams_on_the_background_pool() {
    let bind_address = ("localhost", 0).to_socket_addrs().unwrap().next().unwrap();
    let upstream = TcpListener::bind(bind_address).unwrap();
    let upstream_port = upstream.local_addr().unwrap().port();
    let upstream_thread = thread::spawn(move || {
        let (mut stream, _) = upstream.accept().unwrap();
        let mut request = [0_u8; 1024];
        let read = stream.read(&mut request).unwrap();
        assert!(request[..read]
            .windows(4)
            .any(|window| window == b"\r\n\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nresolved")
            .unwrap();
    });
    let worker = start_worker_with_actions(
        &[(
            "proxy.test",
            Action::Proxy {
                upstream: Some(format!("http://localhost:{upstream_port}")),
                upstream_group: None,
                upstreams: Vec::new(),
                policy: BalancePolicy::default(),
            },
        )],
        WorkerLimits::default(),
    );

    let response = request(worker.address, "proxy.test");
    assert!(response.ends_with("\r\n\r\nresolved"));
    upstream_thread.join().unwrap();
}

#[test]
fn returns_gateway_timeout_when_upstream_response_stalls() {
    let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_thread = thread::spawn(move || {
        let (mut stream, _) = upstream.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
        }
        thread::sleep(Duration::from_millis(300));
    });
    let worker = start_worker_with_proxy_limits(
        &[(
            "proxy.test",
            Action::Proxy {
                upstream: Some(format!("http://{upstream_address}")),
                upstream_group: None,
                upstreams: Vec::new(),
                policy: BalancePolicy::default(),
            },
        )],
        WorkerLimits::default(),
        ProxyLimits {
            connect_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_millis(25),
        },
    );

    let response = request(worker.address, "proxy.test");
    assert!(
        response.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"),
        "unexpected timeout response: {response:?}"
    );
    upstream_thread.join().unwrap();
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
