mod server;
mod client;

// Include the generated protobuf code
pub mod task {
    tonic::include_proto!("task");
}

use task::task_service_server::TaskServiceServer;
use server::MyTaskService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let service = MyTaskService::new();

    println!("TaskService listening on {}", addr);

    Server::builder()
        .add_service(TaskServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
