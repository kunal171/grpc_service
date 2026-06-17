mod server;
mod client;

// Include the generated protobuf code
pub mod task {
    tonic::include_proto!("task");
}

use std::env;

use task::task_service_server::TaskServiceServer;
use server::MyTaskService;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let role = args.get(1).map(|s| s.as_str()).unwrap_or("server");
    let addr = "http://127.0.0.1:50051";

    match role {
        "server" => {
            let socket_addr = "127.0.0.1:50051".parse()?;
            let service = MyTaskService::new();
            println!("TaskService listening on {}", socket_addr);
            Server::builder()
                .add_service(TaskServiceServer::new(service))
                .serve(socket_addr)
                .await?;
        }
        "client" => {
            client::run_client(addr).await?;
        }
        _ => eprintln!("Usage: grpc_service [server|client]"),
    }

    Ok(())
}
