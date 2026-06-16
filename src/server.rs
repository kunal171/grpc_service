use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use tonic::{Request, Response, Status};

use crate::task::task_service_server::TaskService;

use crate::task::{
    CreateTaskRequest, DeleteTaskRequest, DeleteTaskResponse,
    GetTaskRequest, ListTasksRequest, ListTasksResponse, Task, TaskStatus,
};

/// In-memory task storage across multiple threads.
/// 
pub struct MyTaskService {
    tasks: Arc<Mutex<HashMap<u64, Task>>>,
    next_id: Arc<Mutex<u64>>,
}

impl MyTaskService {
    pub fn new() -> Self {
        MyTaskService {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        }
    }
}

#[tonic::async_trait]
impl TaskService for MyTaskService {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        let req = request.into_inner();

        if req.title.is_empty() {
            return Err(Status::invalid_argument("Title cannot be empty"));
        }

        let mut id_counter = self.next_id.lock().await;
        let id = format!("task-{}", *id_counter);
        *id_counter += 1;

        drop(id_counter); // Release the lock on next_id

        let task = Task {
            id: id.clone(),
            title: req.title,
            description: req.description,
            status: TaskStatus::Pending as i32,
        };

        let mut tasks = self.tasks.lock().await;
        tasks.insert(id, task.clone());

        Ok(Response::new(task))
    }


    
    async fn list_tasks(
        &self,
        _request: Request<ListTasksRequest>,
    ) -> Result<Response<ListTasksResponse>, Status> {
        let tasks = self.tasks.lock().await;
        let task_list: Vec<Task> = tasks.values().cloned().collect();

        Ok(Response::new(ListTasksResponse { tasks: task_list }))
    }

    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<DeleteTaskResponse>, Status> {
        let id = request.into_inner().id;

        let mut tasks = self.tasks.lock().await;
        if tasks.remove(&id).is_some() {
            Ok(Response::new(DeleteTaskResponse {}))
        } else {
            Err(Status::not_found(format!("task {} not found", id)))
        }
    }
}