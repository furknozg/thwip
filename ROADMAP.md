# Thwip roadmap

`thwip` is an experimental Unix HTTP server and reverse-proxy project. Its
intended shape is a supervising master process that starts CPU-pinned worker
processes. Every worker owns an async event loop and can serve static files,
return configured responses, or proxy requests upstream.

The project can bind worker-owned sockets and, through the `epoll`/`kqueue`
readiness path, parse framed HTTP/1.x requests, select a virtual host from its
`Host` header, return configured fixed responses, and serve small static files.
The initial HTTP upstream proxy streams through bounded buffers with
backpressure and resolves hostnames on a background pool. Keep-alive, chunked
request bodies, upstream pooling/HTTPS, and production hardening for `io_uring`
remain pending. The direct `io_uring` driver can accept, parse, route, and send
HTTP responses, including static files and streaming upstream proxies.

## TLS roadmap

- [x] Add optional client TLS termination through a per-server `ssl` block,
  including PEM certificate/key loading, TLS 1.2/1.3 selection, configured
  cipher suites, handshake deadlines, and graceful `close_notify` shutdown.
- [x] Integrate client TLS with epoll, kqueue, and io_uring while keeping TLS
  session state owned by each accepted connection.
- [x] Add a rustls-client HTTPS ingress test using the checked-in, test-only
  localhost certificate fixture.
- [ ] Add SNI-based certificate selection for virtual hosts sharing a listener.
- [ ] Add certificate reload without worker restart and operational certificate
  expiry/validation reporting.
- [ ] Add HTTPS upstreams: SNI, hostname verification, trust-store controls,
  handshake timeouts, and separately pooled secure connections.
- [ ] Add TLS observability and negative coverage for malformed handshakes,
  unsupported protocol/cipher negotiation, plaintext on TLS listeners, and
  handshake-timeout behavior on every runtime.
- [ ] Add Linux io_uring-specific HTTPS integration coverage; the existing
  ingress test exercises the shared readiness runtime.

## Goals

- One worker process per configured CPU slot, supervised by a master process.
- A shared HTTP/proxy implementation with interchangeable Unix I/O backends.
- Runtime modes:
  - `epoll`: a broadly compatible readiness-based Linux runtime.
  - `io_uring`: a completion-based Linux runtime for kernels and deployments
    that support the required operations.
  - `kqueue`: a readiness-based macOS/BSD runtime.
- Explicit configuration, predictable fallback behavior, and benchmarkable
  performance.

## Current project map

- `crates/core`: shared configuration types and runtime configuration.
- `run/master`: reads configuration, forks workers, and supervises worker exits.
- `run/slave`: CPU-pins workers, creates listeners, and contains runtime,
  connection, HTTP parsing, and routing code.
- `run/slave/src/runtime/readiness`: shared epoll/kqueue event dispatch,
  connection phases, proxy state, and generation-safe event tokens.
- `rginx.toml`: the default configuration read by the executable.
- `examples/config/`: validated examples for each runtime, direct and balanced
  proxying, static sites, mixed routes, and shared-listener virtual hosts.
- `.github/workflows/ci.yml`: formatting, Clippy, and workspace tests on Linux,
  including the real epoll-backed readiness path.

## Runtime roadmap

### Shared work (required by both runtimes)

- [x] Define a runtime-neutral `Runtime` interface with a `WorkerContext` and
  shared `ShutdownHandle`.
- [x] Add shared idle/drain timers and an immediate runtime wake-up path.
- [ ] Extend the shared runtime interface with general-purpose timers, control
  messages, and runtime metrics.
- [x] Move worker startup and async dependencies out of `master` and into
  `slave`; keep the master process runtime-agnostic.
- [x] Add a `runtime` tagged union with `epoll`, `kqueue`, and `io_uring`
  variants.
- [x] Add an `auto` runtime mode: prefer a successfully probed `io_uring` on
  Linux, fall back to `epoll`, and select `kqueue` on macOS/BSD.
- [x] Validate runtime-specific configuration at load time and reject invalid
  queue/buffer sizes with actionable errors.
- [x] Use one listener per unique address in each worker. Every child creates
  its own nonblocking sockets after `fork`, with `SO_REUSEADDR` and
  `SO_REUSEPORT`. The kernel distributes new connections between workers.
- [x] Implement the worker-side listener factory: create/bind/listen sockets
  after `fork`, set nonblocking mode, and return contextual errors for each
  address that cannot be opened.
- [x] Define an explicit `SocketAddr` listener address (IP address plus port)
  instead of treating `listen` as a port-only field; the generated default is
  `0.0.0.0:8089`.
- [x] Group duplicate listen addresses into one listener per worker. Servers
  sharing an address form a listener group; the first is its default server and
  an exact, case-insensitive `Host` match selects another configured server.
- [ ] Add a Linux integration test: start two workers on the same loopback port,
  verify both binds succeed, and verify all listeners close during shutdown.
- [x] Add initial connection lifecycle safeguards: a per-worker connection
  cap, bounded input/output buffers, idle-connection timeout, bounded per-event
  read/write work, and a shutdown drain deadline.
- [x] Configure those safeguards in the shared `[worker]` section, with safe
  defaults and zero-value rejection.
- [ ] Add overload coverage for the configured safeguards; fragmented requests,
  slow response readers, and client disconnects have live TCP coverage.
- [x] Implement incremental HTTP/1.x request-head parsing with malformed-head
  and request-head-size errors.
- [x] Implement fixed `response` actions with HTTP/1.1 framing and
  `Connection: close`.
- [x] Validate `Content-Length`, wait for complete request bodies before routing,
  reject unsupported transfer encoding, and explicitly close every response.
- [ ] Expose request bodies to actions, implement chunked decoding and
  keep-alive, and complete HTTP response framing semantics.
- [x] Implement small, safe static-file serving: `GET`/`HEAD`, index files,
  query stripping, traversal protection, root containment, MIME types, and an
  in-memory file-size limit.
- [x] Stream large static files through bounded background reads without
  blocking event loops; support single byte ranges, ETags/conditional requests,
  cache headers, and MIME database lookup.
- [x] Implement bounded nonblocking HTTP upstream proxying: forward validated
  request bodies, strip hop-by-hop headers, rewrite `Host`, stream responses
  with client backpressure, and return `502` on upstream failure.
- [x] Define reusable named upstream groups with worker-local round-robin and
  weighted round-robin state; retain direct and inline upstream forms for
  compatibility.
- [x] Move DNS resolution off the worker loop and add generation-safe result
  delivery plus DNS/connect/write/read timeouts.
- [ ] Add upstream response framing validation, pooling, retries, health checks,
  and HTTPS upstreams.
- [x] Add exact/prefix routing with exact-match priority and longest-prefix
  selection.
- [x] Add SIGINT/SIGTERM master-to-worker shutdown and response draining.
- [x] Enforce a configurable graceful-shutdown drain deadline.
- [ ] Add transactional hot reload through generation-based worker replacement,
  with the existing generation retained when validation or startup fails.
- [ ] Add a `thwip_ctl` terminal application over a permission-restricted Unix
  control socket for validation, reload, status, worker, metrics, drain, and
  stop operations.
- [x] Add key-value lifecycle logs, per-worker traffic/error counters, and a
  final shutdown metrics report.
- [x] Restart crashed workers with exponential backoff capped at ten seconds;
  reset the failure sequence after a stable run.
- [ ] Build integration tests that run the same HTTP test suite against both
  runtime modes on their native platforms. Linux epoll runs in CI; macOS/BSD
  kqueue coverage is currently local.

### `epoll` mode

- [x] Use `mio` as the Linux `epoll`-backed readiness implementation.
- [x] Register listening and client sockets for readable events; reregister
  client sockets for writable events only when a response is pending.
- [x] Drain accepts and reads until `EAGAIN`/`WouldBlock` when using edge-triggered
  events.
- [x] Maintain per-connection read/write state and only subscribe to writable
  events while output is pending.
- [x] Handle `EPOLLERR`, `EPOLLHUP`, and `EPOLLRDHUP` through `mio` error/close
  readiness, inspect Linux `SO_ERROR`, retry interrupted syscalls, and reject
  stale events with generation-aware connection tokens.
- [x] Wake the readiness poll immediately for shutdown/control through
  `mio::Waker` (eventfd-style on Linux and the native kqueue equivalent).
- [x] Test fragmented requests, slow response readers/partial writes, clean
  disconnects, half-closes, and forced read/write resets against the live
  readiness worker.
- [ ] Test file descriptor exhaustion.

### `kqueue` mode

- [x] Reuse the readiness worker through `mio` on macOS/BSD.
- [ ] Test multi-worker `SO_REUSEPORT` behavior and graceful shutdown on macOS
  and at least one BSD target.
- [x] Treat CPU affinity as optional on macOS/BSD; workers log that the OS
  scheduler is being used instead of failing startup.

### `io_uring` mode

- [x] Probe and validate the baseline operations required by the direct
  `io_uring` worker before listeners are started; explicit `io_uring` startup
  fails when a required operation is unavailable.
- [x] Use those capability results in `auto` mode so unsupported Linux hosts
  fall back to `epoll` with a clear reason.
- [x] Use a direct `io-uring` driver whose worker owns the ring, listeners,
  accepted sockets, operation state, and buffers.
- [x] Create the ring from configured SQ/CQ entry counts and surface setup
  errors.
- [x] Encode operation kind, slot, and generation in SQE/CQE `user_data`, and
  reject invalid or stale listener completions.
- [x] Implement single-shot accept with one outstanding operation per listener,
  safe accepted-FD ownership, and accept resubmission.
- [x] Retain accepted sockets in a generation-aware connection slab and enforce
  the connection cap.
- [x] Submit one `Recv` per connection, copy completed bytes into an HTTP input
  buffer, handle EOF/errors, and reject stale read completions.
- [x] Parse complete HTTP requests, select virtual hosts/routes, and build the
  same responses as the readiness worker.
- [x] Return HTTP 400/413/501 responses for invalid framing, oversized requests,
  and unsupported transfer encodings instead of silently disconnecting.
- [x] Submit `Send` operations with correct partial-write offsets, bounded
  output, and stale write-completion rejection.
- [x] Add static-file and upstream-proxy parity, including DNS and timeout
  behavior shared with the readiness runtime.
- [x] Drive upstream connect/send/receive through `io_uring`, retain all DNS
  results, and fall back to the next address after a failed connection attempt.
- [x] Flush and retry submissions when the SQ is temporarily saturated instead
  of terminating the worker with `WouldBlock`.
- [x] Add multishot accept behind capability checks while retaining the
  single-shot resubmission fallback.
- [x] Receive client and upstream data through a registered provided-buffer
  ring sized by `buf_ring_size`, with buffers sized by `buf_size` and returned
  after bytes are copied into connection-owned state.
- [x] Wake the ring on shutdown, cancel listener accepts, drain active requests,
  and enforce the configured graceful-shutdown deadline.
- [x] Verify the direct driver with a native Linux smoke run.
- [ ] Add native Linux tests for operation encoding, accept/resubmission,
  connection-slot reuse, stale completions, HTTP parity, and shutdown.
- [ ] Evaluate optional operations (`recvmsg`, `send_zc`, fixed files, splice)
  behind capability checks, not as baseline requirements.
- [ ] Measure queue saturation, completion latency, buffer starvation, and
  cancellation races under load.

### Selection, safety, and performance

- [x] Make explicitly configured `auto` prefer `io_uring` only when its
  required features pass probing, otherwise use `epoll`; omitted runtime
  configuration continues to default to `epoll`.
- [x] Report the selected runtime and fallback reason in startup logs.
- [ ] Expose the selected runtime and fallback reason through metrics.
- [ ] Add parity tests so behavior and HTTP results match across backends.
- [ ] Add benchmarks for small/large responses, keep-alive, proxy streaming,
  slow clients, and overload; publish CPU, latency, and throughput results.
- [ ] Run sanitizers/valgrind-style checks where applicable and stress tests for
  cancellation, worker restart, and shutdown.

## Configuration TODOs

- [x] Use `rginx.toml` consistently as the executable's default configuration.
- [x] Group duplicate `listen` endpoints for virtual hosts; unknown or missing
  `Host` values use the first server configured for that endpoint.
- [ ] Add an explicit schema/version; examples exist for all runtime modes.
- [ ] Validate server names, route paths, response status codes, directories,
  upstream URLs, and worker count.
- [x] Add configurable connection limits, read/write buffer limits, idle
  timeout, and graceful-drain timeout.
- [x] Add independent upstream connect, request-write, and response-read
  timeouts. Pre-response failures return `504 Gateway Timeout`.
- [x] Resolve upstream hostnames on a configurable per-worker background pool,
  wake the readiness loop on completion, ignore stale generation-tagged
  results, and enforce a separate DNS timeout.
- [x] Add named weighted upstream-group settings, reference validation, and
  positive-weight validation.
- [ ] Add separate header/body limits, logging controls, and connection-pool
  settings.

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
- [ ] Add project maturity/status, non-goals, contribution guidelines, and a
  changelog/release policy.
- [x] Adopt the Apache-2.0 license.

## Suggested delivery order

1. Add upstream response-framing validation.
2. Add `auto` runtime selection, capability-based `epoll` fallback, and startup
   logs explaining the selected backend.
3. Add native Linux `io_uring` tests for multishot accept/fallback, provided
   buffers, stale completions, proxying, and graceful shutdown.
4. Expose validated request bodies to other actions, then add keep-alive and
   chunked request decoding.
5. Add multi-worker `SO_REUSEPORT` and shutdown tests on Linux and macOS/BSD,
   plus overload, queue-saturation, buffer-starvation, cancellation-race, and
   file-descriptor-exhaustion coverage.
6. Stream large static files, add range/cache support, and benchmark all runtime
   backends before adopting optional `io_uring` operations.
