use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_stream::iter;
use tokio_stream::StreamExt;

use grpc_service::task::{
    task_operation::Operation,
    task_service_client::TaskServiceClient,
    task_service_server::TaskServiceServer,
    CreateTaskRequest, DeleteTaskRequest, GetTaskRequest,
    ListTasksRequest, TaskOperation, WatchTasksRequest,
};
use grpc_service::server::MyTaskService;
use tonic::transport::Server;

// --- Helpers ---

/// Binds to a random port, starts the gRPC server, returns the address.
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(TaskServiceServer::new(MyTaskService::new()))
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });

    // Give the server a moment to be ready
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

/// Connects a gRPC client to the given address.
async fn make_client(addr: SocketAddr) -> TaskServiceClient<tonic::transport::Channel> {
    TaskServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}

// --- Tests ---

/// Creating a task returns it with the correct title and a generated id.
#[tokio::test]
async fn test_create_and_get_task() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let created = client
        .create_task(CreateTaskRequest {
            title: "test task".to_string(),
            description: "some description".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(created.title, "test task");
    assert!(!created.id.is_empty());

    let fetched = client
        .get_task(GetTaskRequest { id: created.id.clone() })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.title, "test task");
    assert_eq!(fetched.description, "some description");
}

/// Creating a task with an empty title returns InvalidArgument.
#[tokio::test]
async fn test_create_requires_title() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let result = client
        .create_task(CreateTaskRequest {
            title: "".to_string(),
            description: "no title".to_string(),
        })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

/// Getting a non-existent task returns NotFound.
#[tokio::test]
async fn test_get_nonexistent_task() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let result = client
        .get_task(GetTaskRequest { id: "does-not-exist".to_string() })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

/// ListTasks returns all created tasks.
#[tokio::test]
async fn test_list_tasks() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    for i in 0..3 {
        client
            .create_task(CreateTaskRequest {
                title: format!("task-{}", i),
                description: "".to_string(),
            })
            .await
            .unwrap();
    }

    let list = client
        .list_tasks(ListTasksRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(list.tasks.len(), 3);
}

/// Deleting a task removes it — subsequent get returns NotFound.
#[tokio::test]
async fn test_delete_task() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let task = client
        .create_task(CreateTaskRequest {
            title: "to delete".to_string(),
            description: "".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    client
        .delete_task(DeleteTaskRequest { id: task.id.clone() })
        .await
        .unwrap();

    let result = client
        .get_task(GetTaskRequest { id: task.id })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

/// Deleting a non-existent task returns NotFound.
#[tokio::test]
async fn test_delete_nonexistent_task() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let result = client
        .delete_task(DeleteTaskRequest { id: "ghost".to_string() })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

/// BatchTasks creates two tasks and returns a success result for each.
#[tokio::test]
async fn test_batch_create_tasks() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let ops = vec![
        TaskOperation {
            operation: Some(Operation::Create(CreateTaskRequest {
                title: "batch-1".to_string(),
                description: "".to_string(),
            })),
        },
        TaskOperation {
            operation: Some(Operation::Create(CreateTaskRequest {
                title: "batch-2".to_string(),
                description: "".to_string(),
            })),
        },
    ];

    let mut results = client
        .batch_tasks(iter(ops))
        .await
        .unwrap()
        .into_inner();

    let mut count = 0;
    while let Some(Ok(r)) = results.next().await {
        assert!(r.success);
        assert!(r.task.is_some());
        count += 1;
    }
    assert_eq!(count, 2);
}

/// BatchTasks with an empty title returns a failure result (not an error).
#[tokio::test]
async fn test_batch_create_empty_title_returns_failure() {
    let addr = start_server().await;
    let mut client = make_client(addr).await;

    let ops = vec![TaskOperation {
        operation: Some(Operation::Create(CreateTaskRequest {
            title: "".to_string(),
            description: "".to_string(),
        })),
    }];

    let mut results = client
        .batch_tasks(iter(ops))
        .await
        .unwrap()
        .into_inner();

    let r = results.next().await.unwrap().unwrap();
    assert!(!r.success);
    assert!(r.task.is_none());
}

/// WatchTasks receives a CREATED event when a task is created.
#[tokio::test]
async fn test_watch_receives_created_event() {
    let addr = start_server().await;
    let mut watcher = make_client(addr).await;
    let mut actor = make_client(addr).await;

    let mut stream = watcher
        .watch_tasks(WatchTasksRequest {})
        .await
        .unwrap()
        .into_inner();

    // Create a task — should trigger an event on the watcher stream
    actor
        .create_task(CreateTaskRequest {
            title: "watched".to_string(),
            description: "".to_string(),
        })
        .await
        .unwrap();

    let event = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.next(),
    )
    .await
    .expect("timed out waiting for event")
    .unwrap()
    .unwrap();

    assert_eq!(event.task.unwrap().title, "watched");
}
