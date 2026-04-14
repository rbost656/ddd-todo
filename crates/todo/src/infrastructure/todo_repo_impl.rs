use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;

use crate::{
    AppError,
    application::{TodoEventRecord, TodoUnitOfWork, TodoUnitOfWorkManager},
    domain::{
        aggregate::Todo,
        repo::TodoRepository,
        value_objects::TodoId,
    },
};

pub struct InMemoryTodoRepository<U: TodoUnitOfWorkManager> {
    store: Arc<RwLock<HashMap<TodoId, Todo>>>,
    uow_manager: Arc<U>,
}

impl<U: TodoUnitOfWorkManager> InMemoryTodoRepository<U> {
    pub fn new(uow_manager: Arc<U>, store: Arc<RwLock<HashMap<TodoId, Todo>>>) -> Self {
        Self {
            store,
            uow_manager,
        }
    }
}

#[async_trait]
impl<U> TodoRepository for InMemoryTodoRepository<U>
where
    U: TodoUnitOfWorkManager + Send + Sync,
{
    async fn save(&self, mut todo: Todo) -> Result<(), AppError> {
        let uow = self.uow_manager.current()?;
        let uow_id = uow.id().to_string();
        let events = todo.pull_events();

        uow.stage_todo(todo).await?;
        for event in events {
            let event_order = uow.next_event_order().await?;
            let record = TodoEventRecord::new(event, uow_id.clone(), event_order);
            uow.add_event(record).await?;
        }
        Ok(())
    }

    async fn find_by_id(&self, id: &TodoId) -> Result<Option<Todo>, AppError> {
        let todos = self.store.read().map_err(|_| AppError::Persistence {
            message: "todo repository read lock poisoned".to_string(),
        })?;
        Ok(todos.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<Todo>, AppError> {
        let todos = self.store.read().map_err(|_| AppError::Persistence {
            message: "todo repository read lock poisoned".to_string(),
        })?;

        let mut items: Vec<Todo> = todos.values().cloned().collect();
        items.sort_by(|left, right| left.id().as_str().cmp(right.id().as_str()));
        Ok(items)
    }
}

impl<U> InMemoryTodoRepository<U>
where
    U: TodoUnitOfWorkManager + Send + Sync,
{
    pub fn persist(&self, todo: Todo) -> Result<(), AppError> {
        let mut todos = self.store.write().map_err(|_| AppError::Persistence {
            message: "todo repository write lock poisoned".to_string(),
        })?;
        todos.insert(todo.id().clone(), todo);
        Ok(())
    }
}
