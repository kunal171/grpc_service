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
    
    Ok(())
}