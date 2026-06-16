# gRPC Service

A task management service built in Rust using Tonic (gRPC) and Protocol Buffers,
supporting unary RPCs, server streaming, and bidirectional streaming.

## What It Will Do

- Define a task management API using Protocol Buffers
- Implement unary RPCs: create, get, list, delete tasks
- Server streaming: watch for task status changes in real-time
- Bidirectional streaming: batch task operations with live progress
- In-memory storage with shared state
- CLI client for interacting with the service

## Architecture

```text
Client ──gRPC──→ Server (127.0.0.1:50051)
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
└── server.rs     — TaskService trait implementation (create, get, list, delete)
build.rs          — tonic-build protobuf code generation
```

## Implemented So Far

- `proto/task.proto` — Task message, TaskStatus enum, 4 unary RPCs (CreateTask, GetTask, ListTasks, DeleteTask)
- `build.rs` — compiles `.proto` to Rust code at build time via tonic-build
- `server.rs` — `MyTaskService` with in-memory `HashMap<String, Task>` storage, input validation, gRPC status codes
- `main.rs` — starts the gRPC server on port 50051

Milestone 1 in progress — server implemented, client pending.

## Dependencies

```toml
[dependencies]
tonic = "0.13"
prost = "0.13"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[build-dependencies]
tonic-build = "0.13"
```

## Commands

```bash
cargo build       # triggers protobuf code generation via build.rs
cargo run          # start the server on 127.0.0.1:50051
cargo check
cargo test
cargo fmt --check
cargo clippy
```
