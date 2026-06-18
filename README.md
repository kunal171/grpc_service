# gRPC Service

A task management service built in Rust using Tonic (gRPC) and Protocol Buffers,
supporting unary RPCs, server streaming, and bidirectional streaming.

## What It Does

- Defines a task management API using Protocol Buffers
- Implements unary RPCs: create, get, list, delete tasks
- In-memory storage with shared state
- CLI client for interacting with all endpoints
- Server streaming and bidirectional streaming (Milestones 2–3)

## Architecture

```text
Client ──gRPC──→ Server (127.0.0.1:50051)
                   │
             TaskService
                   │
            In-memory store
          (Arc<Mutex<HashMap<String, Task>>>)
```

## Project Structure

```text
proto/
└── task.proto    — Protocol Buffer service and message definitions
src/
├── main.rs       — CLI dispatch: server or client
├── server.rs     — TaskService trait implementation
└── client.rs     — gRPC client that exercises all RPCs
build.rs          — tonic-build protobuf code generation
```

## Wire Protocol

Defined in `proto/task.proto`. Messages:

- `Task` — id, title, description, status (Pending/InProgress/Completed)
- `CreateTaskRequest` — title, description
- `GetTaskRequest` — id
- `ListTasksRequest` — (empty)
- `DeleteTaskRequest` — id

Service RPCs:

```protobuf
service TaskService {
  rpc CreateTask(CreateTaskRequest) returns (Task);              // unary
  rpc GetTask(GetTaskRequest) returns (Task);                    // unary
  rpc ListTasks(ListTasksRequest) returns (ListTasksResponse);   // unary
  rpc DeleteTask(DeleteTaskRequest) returns (DeleteTaskResponse); // unary
  rpc WatchTasks(WatchTasksRequest) returns (stream TaskEvent);  // server streaming
}
```

## Usage

```bash
# Terminal 1: start the server
cargo run -- server

# Terminal 2: run the client
cargo run -- client
```

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
cargo run -- server
cargo run -- client
cargo check
cargo test
cargo fmt --check
cargo clippy
```
