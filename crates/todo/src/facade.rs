use crate::{
    DomainError,
    application::{
        CompleteTodo, CreateTodo, GetTodo, ListTodos, TodoProjectionService, TodoService, TodoView,
    },
    infrastructure::{
        InMemoryTodoEventInbox, InMemoryTodoEventOutbox, InMemoryTodoOutboxRelay,
        InMemoryTodoProjectionStore, InMemoryTodoRepository, InMemoryTodoUnitOfWorkManager,
    },
};

type TodoUowManager = InMemoryTodoUnitOfWorkManager<InMemoryTodoEventOutbox>;
type TodoRepo = InMemoryTodoRepository<TodoUowManager>;
type TodoRelay =
    InMemoryTodoOutboxRelay<InMemoryTodoEventOutbox, InMemoryTodoEventInbox, InMemoryTodoProjectionStore>;

pub struct TodoFacade {
    service: TodoService<TodoRepo, TodoUowManager, InMemoryTodoProjectionStore>,
    projection_service: TodoProjectionService<TodoRelay>,
}

impl TodoFacade {
    pub fn new(
        service: TodoService<TodoRepo, TodoUowManager, InMemoryTodoProjectionStore>,
        projection_service: TodoProjectionService<TodoRelay>,
    ) -> Self {
        Self {
            service,
            projection_service,
        }
    }

    pub async fn create_todo(
        &self,
        id: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<TodoView, DomainError> {
        let id = id.into();
        let title = title.into();
        self.service
            .create_todo(CreateTodo {
                id: id.clone(),
                title,
            })
            .await?;
        self.projection_service.flush_outbox().await?;
        self.get_todo(id).await
    }

    pub async fn complete_todo(&self, id: impl Into<String>) -> Result<TodoView, DomainError> {
        let id = id.into();
        self.service
            .complete_todo(CompleteTodo { id: id.clone() })
            .await?;
        self.projection_service.flush_outbox().await?;
        self.get_todo(id).await
    }

    pub async fn get_todo(&self, id: impl Into<String>) -> Result<TodoView, DomainError> {
        self.service.get_todo(GetTodo { id: id.into() }).await
    }

    pub async fn list_todos(&self) -> Result<Vec<TodoView>, DomainError> {
        self.service.list_todos(ListTodos).await
    }
}
