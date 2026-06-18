use tokio_stream::{StreamExt, iter};

use crate::task::task_service_client::TaskServiceClient;
use crate::task::{CreateTaskRequest, DeleteTaskRequest, GetTaskRequest, ListTasksRequest, WatchTasksRequest,TaskOperation, task_operation::Operation};

pub async fn run_client(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TaskServiceClient::connect(addr.to_string()).await?;
    println!("[client] connected to {}", addr);

    // 1. Create a new task
    let task1 = client
        .create_task(CreateTaskRequest {
            title: "learn Rust".to_string(),
            description: "Go through Rust book and practice".to_string(),
        })
        .await?
        .into_inner();
    println!("[client] created task: {:?}", task1);

    let task2 = client
        .create_task(CreateTaskRequest {
            title: "Write tests".to_string(),
            description: "Integration tests for the gRPC service".to_string(),
        })
        .await?
        .into_inner();
    println!("[client] created: {} — {}", task2.id, task2.title);

    //2. Get a task by ID

    let fetched = client
        .get_task(GetTaskRequest { id: task1.id.clone() })
        .await?
        .into_inner();
    println!("[client] fetched task: {:?}", fetched);

// 3. List all tasks
    let all = client
        .list_tasks(ListTasksRequest {})
        .await?
        .into_inner();
    println!("[client] total tasks: {}", all.tasks.len());
    for t in &all.tasks {
        println!("  {} — {} (status: {})", t.id, t.title, t.status);
    }

    // 4. Delete a task
    client
        .delete_task(DeleteTaskRequest { id: task2.id.clone() })
        .await?;
    println!("[client] deleted: {}", task2.id);

    // 5. List again to confirm deletion
    let remaining = client
        .list_tasks(ListTasksRequest {})
        .await?
        .into_inner();
    println!("[client] tasks after delete: {}", remaining.tasks.len());

    //6. Watch for task events
    println!("[client] starting to watch for task events...");
    let mut stream = client.watch_tasks(WatchTasksRequest{})
        .await?
        .into_inner();  

    // Spawn a task to listen for events
    let watch_handle = tokio::spawn(async move {
        while let Some(event) = stream.next().await {
           match event {
               Ok(e) => {
                    let task = e.task.unwrap_or_default();
                    let event_type = if e.event_type == 0 { "Created" } else { "Deleted" };
                    println!("[client] event: {} — {} (status: {})", event_type, task.title, task.status);
                }
                Err(e) => {
                    eprintln!("[client] error receiving event: {:?}", e);
               }
           }
        }
    });


    // Create and delete a task while the watcher is running
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let new_task = client.create_task(CreateTaskRequest {
        title: "watched task".to_string(),
        description: "should trigger a watcher event".to_string(),
    }).await?.into_inner();

    client.delete_task(DeleteTaskRequest { id: new_task.id }).await?;

    // Give watcher time to receive events then stop
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    watch_handle.abort();

    //7. Batch operations (create multiple tasks in one request)
    println!("[client] starting batch operations...");

    // Build a stream of operations to send
    let operations = vec![
        TaskOperation {
            operation: Some(Operation::Create(CreateTaskRequest {
                title: "batch task 1".to_string(),
                description: "created via batch".to_string(),
            })),
        },
        TaskOperation {
            operation: Some(Operation::Create(CreateTaskRequest {
                title: "batch task 2".to_string(),
                description: "created via batch".to_string(),
            })),
        },
        TaskOperation {
            operation: Some(Operation::Delete(DeleteTaskRequest {
                id: task1.id.clone(),
            })),
        },
    ];

    let mut results = client
        .batch_tasks(iter(operations))
        .await?
        .into_inner();

    while let Some(result) = results.next().await {
        match result {
            Ok(r) => {
                let task_info = r.task.map(|t| format!(" — {}", t.title)).unwrap_or_default();
                println!("[batch] success={} msg={}{}", r.success, r.message, task_info);
            }
            Err(e) => println!("[batch] error: {}", e),
        }
    }
    Ok(())
}