use crate::task::task_service_client::TaskServiceClient;
use crate::task::{CreateTaskRequest, GetTaskRequest, ListTasksRequest, DeleteTaskRequest};

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

    
    Ok(())
}