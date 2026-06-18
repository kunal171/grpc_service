use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use tonic::{Request, Response, Status, Streaming};

use crate::task::task_service_server::TaskService;

use crate::task::{
    CreateTaskRequest, DeleteTaskRequest, DeleteTaskResponse,
    GetTaskRequest, ListTasksRequest, ListTasksResponse, Task, TaskStatus, EventType, TaskEvent,
    WatchTasksRequest, TaskResult, TaskOperation, task_operation::Operation
};

type WatchStream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TaskEvent, Status>> + Send>>; 

/// In-memory task storage across multiple threads.
pub struct MyTaskService {
    tasks: Arc<Mutex<HashMap<String, Task>>>,
    next_id: Arc<Mutex<u64>>,
    // Sender half — broadcast task events to all active watchers
    event_tx: broadcast::Sender<TaskEvent>,
}

impl MyTaskService {
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(100); // Buffer size for task events
        MyTaskService {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            event_tx,
        }
    }
}

#[tonic::async_trait]
impl TaskService for MyTaskService {
    
    type BatchTasksStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<TaskResult, Status>> + Send>
    >;
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

        let _ = self.event_tx.send(TaskEvent {
            event_type: EventType::Created as i32,
            task: Some(task.clone()),
        });

        Ok(Response::new(task))
    }


    async fn get_task(
        &self,
        request: Request<GetTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        let id = request.into_inner().id;

        let tasks = self.tasks.lock().await;
        match tasks.get(&id) {
            Some(task) => Ok(Response::new(task.clone())),
            None => Err(Status::not_found(format!("task {} not found", id))),
        }
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
            let _ = self.event_tx.send(TaskEvent {
                event_type: EventType::Deleted as i32,
                task: Some(Task { id: id.clone(), ..Default::default() }),
            });
            Ok(Response::new(DeleteTaskResponse {}))
        } else {
            Err(Status::not_found(format!("task {} not found", id)))
        }
    }
    
    type WatchTasksStream = WatchStream;
    async fn watch_tasks(
        &self,
        _request: Request<WatchTasksRequest>,
    ) -> Result<Response<Self::WatchTasksStream>, Status> {
        let rx = self.event_tx.subscribe();

        // Wrap the broadcast receiver in a Stream, map errors to gRPC Status
        let stream = BroadcastStream::new(rx).map(|result| {
            result.map_err(|e| Status::internal(format!("stream error: {}", e)))
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn batch_tasks(
        &self,
        request: Request<Streaming<TaskOperation>>,
    ) -> Result<Response<Self::BatchTasksStream>, Status> {
        let mut inbound = request.into_inner();

        // Clone what we need to move into the stream
        let tasks = self.tasks.clone();
        let next_id = self.next_id.clone();
        let event_tx = self.event_tx.clone();

        let output = async_stream::try_stream! {
            while let Some(op) = inbound.message().await? {
                let result = match op.operation {
                    Some(Operation::Create(req)) => {
                        if req.title.is_empty() {
                            TaskResult {
                                success: false,
                                message: "title is required".to_string(),
                                task: None,
                            }
                        } else {
                            let mut id_counter = next_id.lock().await;
                            let id = format!("task-{}", *id_counter);
                            *id_counter += 1;
                            drop(id_counter);

                            let task = Task {
                                id: id.clone(),
                                title: req.title,
                                description: req.description,
                                status: TaskStatus::Pending as i32,
                            };
                            tasks.lock().await.insert(id, task.clone());
                            let _ = event_tx.send(TaskEvent {
                                event_type: EventType::Created as i32,
                                task: Some(task.clone()),
                            });
                            TaskResult {
                                success: true,
                                message: "created".to_string(),
                                task: Some(task),
                            }
                        }
                    }
                    Some(Operation::Delete(req)) => {
                        let removed = tasks.lock().await.remove(&req.id);
                        if let Some(task) = removed {
                            let _ = event_tx.send(TaskEvent {
                                event_type: EventType::Deleted as i32,
                                task: Some(task.clone()),
                            });
                            TaskResult {
                                success: true,
                                message: format!("deleted {}", req.id),
                                task: Some(task),
                            }
                        } else {
                            TaskResult {
                                success: false,
                                message: format!("task {} not found", req.id),
                                task: None,
                            }
                        }
                    }
                    None => TaskResult {
                        success: false,
                        message: "empty operation".to_string(),
                        task: None,
                    },
                };
                yield result;
            }
        };

        Ok(Response::new(Box::pin(output)))
    }
}