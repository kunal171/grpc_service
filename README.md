# gRPC Service

A task management service built in Rust using Tonic (gRPC) and Protocol Buffers,
supporting unary RPCs, server streaming, and bidirectional streaming.

## What It Will Do

- Define a task management API using Protocol Buffers
- Implement unary RPCs: create, get, update, delete tasks
- Server streaming: watch for task status changes in real-time
- Bidirectional streaming: batch task operations with live progress
- In-memory storage with shared state
- Separate server and client binaries

## Architecture

```text
Client ──gRPC──→ Server
                   │
             TaskService
                   │
            In-memory store
          (Arc<Mutex<HashMap>>)
```

## Project Structure

```text
proto/
└── task.proto    — Protocol Buffer service and message definitions
src/
├── main.rs       — gRPC server entry point
├── server.rs     — TaskService implementation
├── client.rs     — CLI client for interacting with the service
└── build.rs      — Protobuf code generation
```

## Dependencies

```toml
[dependencies]
tonic = "0.13"
prost = "0.13"
tokio = { version = "1", features = ["full"] }

[build-dependencies]
tonic-build = "0.13"
```

## Commands

```bash
cargo check
cargo build
cargo test
cargo fmt --check
cargo clippy
```
