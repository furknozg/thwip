# Contributing to Thwip

Thanks for helping improve Thwip. The project is experimental and has several
runtime-specific paths, so a small, clearly tested change is more useful than a
large speculative rewrite.

## Before you start

- Search existing issues and pull requests before opening a new one.
- Open an issue first for a new feature, runtime behavior change, or large
  refactor. Explain the problem, the affected operating systems/runtimes, and
  the proposed shape of the change.
- Security-sensitive issues should not include an exploit or private
  infrastructure details in a public issue. Open a minimal issue asking for a
  private reporting channel instead.

## Development setup

Use a current stable Rust toolchain on Linux, macOS, or a supported BSD target.
From the repository root:

```sh
git clone https://github.com/furknozg/thwip.git
cd thwip
cargo test --workspace
```

The local executable reads `rginx.toml` from the current working directory.
Copy a configuration from `examples/config/` when you need a focused local
setup.

## Making a change

1. Keep the change scoped to one behavior or concern.
2. Add or update tests in the relevant crate's `tests/` directory.
3. Preserve runtime parity where the behavior is shared. If a behavior is
   intentionally platform-specific, state that in the code and pull request.
4. Update `README.md`, `ROADMAP.md`, examples, or `public/docs/` when the user
   visible behavior or configuration changes.
5. Do not commit generated build output or local configuration containing
   credentials.

## Checks

Run these before opening a pull request:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

GitHub Actions also tests the readiness runtime on Linux/epoll and
macOS/kqueue, and compiles plus validates the io_uring configuration on Linux.
If you touch io_uring behavior, test it on a native Linux host when possible.

## Pull requests

Use a concise title and explain:

- the problem and solution;
- affected runtime(s) and operating system(s);
- configuration or documentation changes;
- tests run, including any platform-specific checks not run.

Avoid mixing formatting-only changes with behavior changes. Maintainers may ask
for a split pull request when it makes review or backporting clearer.

## License

By submitting a contribution, you agree that it is provided under the
[Apache License, Version 2.0](LICENSE), unless you explicitly state otherwise
in writing before it is accepted.
