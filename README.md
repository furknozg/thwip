# thwip

`thwip` is an experimental Linux HTTP server and reverse-proxy project. Its
intended shape is a supervising master process that starts CPU-pinned worker
processes. Every worker owns an async event loop and can serve static files,
return configured responses, or proxy requests upstream.

The project can now bind worker-owned sockets and, through the Linux `epoll`
path, parse one HTTP/1.x request head and return configured fixed responses.
Static files, proxying, keep-alive, and the `io_uring` runtime remain pending.

## Goals

- One worker process per configured CPU slot, supervised by a master process.
- A shared HTTP/proxy implementation with interchangeable Linux I/O backends.
- Two runtime modes:
  - `epoll`: a broadly compatible readiness-based Linux runtime.
  - `io_uring`: a completion-based Linux runtime for kernels and deployments
    that support the required operations.
- Explicit configuration, predictable fallback behavior, and benchmarkable
  performance.

## Current project map

- `crates/core`: shared configuration types and runtime configuration.
- `run/master`: reads configuration, forks workers, and supervises worker exits.
- `run/slave`: CPU-pins workers, creates listeners, and contains runtime,
  connection, HTTP parsing, and routing code.
- `rginx.toml`: the default configuration read by the executable.
- `examples/config/`: minimal independent `epoll` and `io_uring` configuration
  examples.

## Runtime roadmap

### Shared work (required by both runtimes)

- [ ] Define a runtime-neutral worker interface: bind/listen, accept, read,
  write, close, timers, wake-ups, and graceful shutdown.
- [x] Move worker startup and async dependencies out of `master` and into
  `slave`; keep the master process runtime-agnostic.
- [x] Add a `runtime` tagged union with `epoll` and `io_uring` variants.
- [ ] Add an `auto` runtime mode once capability probing and fallback exist.
- [ ] Validate runtime-specific configuration at load time and reject invalid
  queue/buffer sizes with actionable errors.
- [x] Use one listener per worker. Each child creates its own nonblocking socket
  after `fork`, with `SO_REUSEADDR` and `SO_REUSEPORT`, and binds every configured
  `listen` address. The kernel distributes new connections between workers.
- [x] Implement the worker-side listener factory: create/bind/listen sockets
  after `fork`, set nonblocking mode, and return contextual errors for each
  address that cannot be opened.
- [x] Define an explicit `SocketAddr` listener address (IP address plus port)
  instead of treating `listen` as a port-only field; the generated default is
  `0.0.0.0:8089`.
- [x] Validate duplicate listen addresses once per config, while allowing every
  worker to bind the same address through `SO_REUSEPORT`.
- [ ] Add a Linux integration test: start two workers on the same loopback port,
  verify both binds succeed, and verify all listeners close during shutdown.
- [ ] Implement connection lifecycle and backpressure limits (maximum open
  connections, input/output buffer limits, and request timeouts).
- [x] Implement incremental HTTP/1.x request-head parsing with malformed-head
  and request-head-size errors.
- [x] Implement fixed `response` actions with HTTP/1.1 framing and
  `Connection: close`.
- [ ] Implement keep-alive, request bodies, request-size limits, and complete
  HTTP response framing semantics.
- [ ] Implement safe static-file serving and streaming upstream proxying.
- [x] Add exact/prefix routing with exact-match priority and longest-prefix
  selection.
- [ ] Add structured logs, per-worker metrics, and graceful drain/shutdown.
- [ ] Restart crashed workers with a bounded backoff and a shutdown signal path.
- [ ] Build integration tests that run the same HTTP test suite against both
  runtime modes.

### `epoll` mode

- [x] Use `mio` as the Linux `epoll`-backed readiness implementation.
- [x] Register listening and client sockets for readable events; reregister
  client sockets for writable events only when a response is pending.
- [x] Drain accepts and reads until `EAGAIN`/`WouldBlock` when using edge-triggered
  events.
- [x] Maintain per-connection read/write state and only subscribe to writable
  events while output is pending.
- [ ] Handle `EPOLLERR`, `EPOLLHUP`, `EPOLLRDHUP`, interrupted syscalls, and
  descriptor reuse safely.
- [ ] Add an `eventfd`/pipe wake-up mechanism for control messages and shutdown.
- [ ] Test slow clients, partial writes, half-closed connections, and file
  descriptor exhaustion.

### `io_uring` mode

- [ ] Probe kernel and opcode support at startup; make `auto` fall back to
  `epoll` and make explicit `io_uring` fail clearly when unsupported.
- [ ] Pick one ownership model: `tokio-uring` operations or a direct
  `io-uring` driver. Do not mix their socket ownership/lifecycle models.
- [ ] Create the ring from configured SQ/CQ entry counts and surface setup
  errors.
- [ ] Implement multishot accept where supported, with a compatible accept
  resubmission path otherwise.
- [ ] Use fixed/provided buffers only after a safe buffer-ownership and return
  protocol is defined; wire `buf_ring_size` and `buf_size` into that design.
- [ ] Implement completion dispatch keyed by connection/operation generation to
  prevent stale completions from affecting reused connections.
- [ ] Define cancellation and shutdown behavior for every outstanding request;
  drain completions before releasing resources.
- [ ] Evaluate optional operations (`recvmsg`, `send_zc`, fixed files, splice)
  behind capability checks, not as baseline requirements.
- [ ] Measure queue saturation, completion latency, buffer starvation, and
  cancellation races under load.

### Selection, safety, and performance

- [ ] Specify the default: `auto` should prefer `io_uring` only when its
  required features pass probing, otherwise use `epoll`.
- [ ] Expose the selected runtime and fallback reason in startup logs and
  metrics.
- [ ] Add parity tests so behavior and HTTP results match across backends.
- [ ] Add benchmarks for small/large responses, keep-alive, proxy streaming,
  slow clients, and overload; publish CPU, latency, and throughput results.
- [ ] Run sanitizers/valgrind-style checks where applicable and stress tests for
  cancellation, worker restart, and shutdown.

## Configuration TODOs

- [x] Use `rginx.toml` consistently as the executable's default configuration.
- [ ] Reject duplicate `listen` endpoints unless the selected listener model
  explicitly supports them. The sample currently declares the same port twice.
- [ ] Add an explicit schema/version and examples for all runtime modes.
- [ ] Validate server names, route paths, response status codes, directories,
  upstream URLs, and worker count.
- [ ] Add timeouts, header/body limits, logging, TLS, and upstream pool settings.

## README/documentation TODOs

- [ ] Add supported platforms, minimum Linux kernel requirements, and the
  `io_uring` capability/fallback policy.
- [ ] Add prerequisites and build/run instructions, including required Rust and
  system dependencies.
- [ ] Add a minimal working configuration and a `curl` verification example.
- [ ] Document every configuration field, defaults, units, validation rules,
  and route-matching precedence.
- [ ] Explain process topology, CPU affinity, socket/listener ownership, and
  how workers are restarted.
- [ ] Document runtime selection, when to choose `epoll` vs. `io_uring`, and
  known behavioral/performance trade-offs.
- [ ] Define the HTTP feature matrix: supported methods, HTTP versions,
  keep-alive, chunked bodies, WebSockets, TLS, and unsupported features.
- [ ] Document static-file security rules (path traversal, symlinks, MIME types,
  directory listings, and cache headers).
- [ ] Document proxy behavior (DNS, connection pooling, retries, timeouts,
  forwarded headers, buffering, and streaming).
- [ ] Add observability, security, deployment, troubleshooting, testing, and
  benchmarking sections.
- [ ] Add project maturity/status, non-goals, contribution guidelines, license,
  and a changelog/release policy.

## Suggested delivery order

1. Add Linux end-to-end tests for listener binding, a fixed response, partial
   reads/writes, and controlled shutdown.
2. Add graceful shutdown, connection limits/timeouts, and robust error-event
   handling to the `epoll` worker.
3. Implement HTTP bodies and keep-alive, then static-file serving and proxying.
4. Introduce the runtime-neutral interface and implement `io_uring` behind it.
5. Add capability probing, fallback, parity tests, and comparative benchmarks.
