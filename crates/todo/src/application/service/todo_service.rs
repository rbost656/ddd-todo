use crate::{
    application::{
        dto::{CompleteTodo, CreateTodo, GetTodo, ListTodos, TodoView},
        handler::{CommandHandler, QueryHandler},
        projection::TodoProjectionStore,
    },
    domain::{
        builders::TodoBuilder,
        model::Builder,
        repo::TodoRepository,
        uow::TodoUnitOfWorkManager,
        value_objects::TodoId,
    },
    DomainError,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct TodoService<R, U, Q>
where
    R: TodoRepository,
    U: TodoUnitOfWorkManager,
    Q: TodoProjectionStore,
{
    repo: Arc<R>,
    uow: Arc<U>,
    projection_store: Arc<Q>,
    todo_builder: TodoBuilder,
}

impl<R, U, Q> TodoService<R, U, Q>
where
    R: TodoRepository,
    U: TodoUnitOfWorkManager,
    Q: TodoProjectionStore,
{
    pub fn new(repo: Arc<R>, uow: Arc<U>, projection_store: Arc<Q>) -> Self {
        Self {
            repo,
            uow,
            projection_store,
            todo_builder: TodoBuilder,
        }
    }

    pub async fn create_todo(&self, cmd: CreateTodo) -> Result<TodoView, DomainError> {
        <Self as CommandHandler<CreateTodo>>::handle(self, cmd).await
    }

    pub async fn complete_todo(&self, cmd: CompleteTodo) -> Result<TodoView, DomainError> {
        <Self as CommandHandler<CompleteTodo>>::handle(self, cmd).await
    }

    pub async fn get_todo(&self, query: GetTodo) -> Result<TodoView, DomainError> {
        <Self as QueryHandler<GetTodo>>::handle(self, query).await
    }

    pub async fn list_todos(&self, query: ListTodos) -> Result<Vec<TodoView>, DomainError> {
        <Self as QueryHandler<ListTodos>>::handle(self, query).await
    }
}

#[async_trait]
impl<R, U, Q> CommandHandler<CreateTodo> for TodoService<R, U, Q>
where
    R: TodoRepository + Send + Sync,
    U: TodoUnitOfWorkManager + Send + Sync,
    Q: TodoProjectionStore + Send + Sync,
{
    type Output = TodoView;

    async fn handle(&self, cmd: CreateTodo) -> Result<Self::Output, DomainError> {
        let id = TodoId::new(cmd.id.clone())?;
        if self.repo.find_by_id(&id).await?.is_some() {
            return Err(DomainError::Conflict {
                message: format!("todo {} already exists", id),
            });
        }

        self.uow.begin(|| async {
            let todo = self.todo_builder.build(cmd)?;
            let view = TodoView::from(&todo);
            self.repo.save(todo).await?;
            Ok(view)
        })
        .await
    }
}

#[async_trait]
impl<R, U, Q> CommandHandler<CompleteTodo> for TodoService<R, U, Q>
where
    R: TodoRepository + Send + Sync,
    U: TodoUnitOfWorkManager + Send + Sync,
    Q: TodoProjectionStore + Send + Sync,
{
    type Output = TodoView;

    async fn handle(&self, cmd: CompleteTodo) -> Result<Self::Output, DomainError> {
        let id = TodoId::new(cmd.id)?;
        let mut todo = self.repo.find_by_id(&id).await?.ok_or_else(|| DomainError::NotFound {
            message: format!("todo {} does not exist", id),
        })?;

        self.uow.begin(|| async {
            todo.complete()?;
            let view = TodoView::from(&todo);
            self.repo.save(todo).await?;
            Ok(view)
        })
        .await
    }
}

#[async_trait]
impl<R, U, Q> QueryHandler<GetTodo> for TodoService<R, U, Q>
where
    R: TodoRepository + Send + Sync,
    U: TodoUnitOfWorkManager + Send + Sync,
    Q: TodoProjectionStore + Send + Sync,
{
    type Output = TodoView;

    async fn handle(&self, query: GetTodo) -> Result<Self::Output, DomainError> {
        let id = TodoId::new(query.id)?;
        self.projection_store.get(id.as_str()).await?.ok_or_else(|| DomainError::NotFound {
            message: format!("todo {} does not exist", id),
        })
    }
}

#[async_trait]
impl<R, U, Q> QueryHandler<ListTodos> for TodoService<R, U, Q>
where
    R: TodoRepository + Send + Sync,
    U: TodoUnitOfWorkManager + Send + Sync,
    Q: TodoProjectionStore + Send + Sync,
{
    type Output = Vec<TodoView>;

    async fn handle(&self, _query: ListTodos) -> Result<Self::Output, DomainError> {
        self.projection_store.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::TodoService;
    use crate::application::{
        dto::{CompleteTodo, CreateTodo, GetTodo, ListTodos},
        TodoProjectionService,
    };
    use crate::{
        DomainError,
        domain::{
            aggregate::Todo,
            outbox::{TodoEventInbox, TodoEventOutbox},
            value_objects::TodoId,
        },
    };
    use crate::infrastructure::{
        InMemoryTodoEventInbox, InMemoryTodoEventOutbox, InMemoryTodoOutboxRelay,
        InMemoryTodoProjectionStore, InMemoryTodoRepository, InMemoryTodoUnitOfWorkManager,
    };
    use std::{collections::HashMap, sync::{Arc, RwLock}};

    #[tokio::test]
    async fn todo_service_implements_cqrs_handlers_directly() {
        let store = Arc::new(RwLock::new(HashMap::<TodoId, Todo>::new()));
        let outbox = Arc::new(InMemoryTodoEventOutbox::default());
        let inbox = Arc::new(InMemoryTodoEventInbox::default());
        let projection_store = Arc::new(InMemoryTodoProjectionStore::default());
        let persist_store = store.clone();
        let uow = Arc::new(InMemoryTodoUnitOfWorkManager::new(
            Arc::new(move |todo| {
                let persist_store = persist_store.clone();
                Box::pin(async move {
                    let mut todos = persist_store.write().map_err(|_| DomainError::Persistence {
                        message: "todo persistent store write lock poisoned".to_string(),
                    })?;
                    todos.insert(todo.id().clone(), todo);
                    Ok(())
                })
            }),
            outbox.clone(),
        ));
        let repo = Arc::new(InMemoryTodoRepository::new(uow.clone(), store));
        let relay = Arc::new(InMemoryTodoOutboxRelay::new(
            outbox.clone(),
            inbox.clone(),
            projection_store.clone(),
        ));
        let service = TodoService::new(repo, uow, projection_store.clone());
        let projection_service = TodoProjectionService::new(relay);

        let created = service
            .create_todo(CreateTodo {
                id: "todo-1".to_string(),
                title: "introduce cqrs".to_string(),
            })
            .await
            .expect("todo created");
        projection_service
            .flush_outbox()
            .await
            .expect("flush create outbox");
        assert_eq!(created.id, "todo-1");
        assert!(!created.completed);

        let loaded = service
            .get_todo(GetTodo {
                id: "todo-1".to_string(),
            })
            .await
            .expect("todo loaded");
        assert_eq!(loaded, created);

        let completed = service
            .complete_todo(CompleteTodo {
                id: "todo-1".to_string(),
            })
            .await
            .expect("todo completed");
        projection_service
            .flush_outbox()
            .await
            .expect("flush complete outbox");
        assert!(completed.completed);

        let listed = service
            .list_todos(ListTodos)
            .await
            .expect("todos listed");
        assert_eq!(listed, vec![completed]);
        assert_eq!(listed[0].version, 2);

        let pending = outbox.pending().await.expect("pending outbox records");
        assert!(pending.is_empty());
        assert!(inbox
            .contains("todo-1:1:todo.created")
            .await
            .expect("inbox create"));
        assert!(inbox
            .contains("todo-1:2:todo.completed")
            .await
            .expect("inbox complete"));
    }
}
