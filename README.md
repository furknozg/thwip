# thwip

`thwip` is an experimental Linux HTTP server and reverse-proxy project. Its
intended shape is a supervising master process that starts CPU-pinned worker
processes. Every worker owns an async event loop and can serve static files,
return configured responses, or proxy requests upstream.

The project is currently a skeleton: configuration parsing and the master/
worker process outline exist, but it does not yet bind sockets or serve HTTP
traffic.

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
- `run/master`: reads configuration, forks workers, pins them to CPUs, and
  waits for worker exits.
- `run/slave`: intended home for the worker implementation; currently a stub.
- `rginx.toml`: the default configuration read by the executable.
- `examples/config/`: minimal independent `epoll` and `io_uring` configuration
  examples.

## Runtime roadmap

### Shared work (required by both runtimes)

- [ ] Define a runtime-neutral worker interface: bind/listen, accept, read,
  write, close, timers, wake-ups, and graceful shutdown.
- [ ] Move worker startup and async dependencies out of `master` and into
  `slave`; keep the master process runtime-agnostic.
- [x] Add a `runtime` tagged union with `epoll` and `io_uring` variants.
- [ ] Add an `auto` runtime mode once capability probing and fallback exist.
- [ ] Validate runtime-specific configuration at load time and reject invalid
  queue/buffer sizes with actionable errors.
- [ ] Implement socket setup before forking: nonblocking sockets, `SO_REUSEADDR`,
  and `SO_REUSEPORT` when one listener per worker is selected.
- [ ] Decide and document listener ownership: inherited listener vs. one
  `SO_REUSEPORT` listener per worker.
- [ ] Implement connection lifecycle and backpressure limits (maximum open
  connections, input/output buffer limits, and request timeouts).
- [ ] Implement HTTP/1.1 parsing, keep-alive, request-size limits, and correct
  response framing.
- [ ] Implement the configured actions: `response`, safe static-file serving,
  and streaming upstream proxying.
- [ ] Add routing rules for exact/prefix matching and deterministic precedence.
- [ ] Add structured logs, per-worker metrics, and graceful drain/shutdown.
- [ ] Restart crashed workers with a bounded backoff and a shutdown signal path.
- [ ] Build integration tests that run the same HTTP test suite against both
  runtime modes.

### `epoll` mode

- [ ] Choose and document the implementation approach (`mio`, direct `epoll`,
  or a Tokio runtime backed by `epoll`).
- [ ] Register listening and client sockets with edge- or level-triggered
  semantics; document the choice.
- [ ] Correctly drain accepts and reads until `EAGAIN` when using edge-triggered
  events.
- [ ] Maintain per-connection read/write state and only subscribe to writable
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

1. Finish configuration validation and a single listener strategy.
2. Implement the shared HTTP connection state machine and an `epoll` worker.
3. Add full end-to-end tests and make `epoll` reliable under failure cases.
4. Introduce the runtime-neutral interface and implement `io_uring` behind it.
5. Add capability probing, fallback, parity tests, and comparative benchmarks.
