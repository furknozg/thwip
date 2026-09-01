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
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};

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

#[test]
fn routes_shared_listener_by_host_and_uses_its_default_server() {
    let requested_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = TcpListener::bind(requested_address).expect("bind listener");
    socket
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    let address = socket.local_addr().expect("read bound address");
    let shutdown = ShutdownHandle::new();
    let context = WorkerContext {
        listener_groups: vec![BoundListenerGroup {
            socket,
            address,
            default_server: 0,
            server_indices: vec![0, 1],
        }],
        servers: vec![
            server("one.test", "one", address),
            server("two.test", "two", address),
        ],
        shutdown: shutdown.clone(),
        limits: WorkerLimits::default(),
    };

    let worker = thread::spawn(move || run_readiness(context));

    assert!(request(address, "one.test").ends_with("\r\n\r\none"));
    assert!(request(address, "TWO.TEST:8080").ends_with("\r\n\r\ntwo"));
    assert!(request(address, "unknown.test").ends_with("\r\n\r\none"));

    shutdown.request();
    worker
        .join()
        .expect("worker thread should not panic")
        .expect("worker should shut down cleanly");
}
