use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::{
    DomainError,
    domain::{
        aggregate::Todo,
        outbox::{TodoEventOutbox, TodoEventRecord},
        uow::{TodoUnitOfWork, TodoUnitOfWorkManager},
    },
};

thread_local! {
    static CURRENT_UOW_STACK: RefCell<Vec<Arc<InMemoryTodoUnitOfWork>>> = const { RefCell::new(Vec::new()) };
}

#[derive(Default)]
struct UnitOfWorkState {
    todos: Vec<Todo>,
    records: Vec<TodoEventRecord>,
}

pub struct InMemoryTodoUnitOfWork {
    state: Mutex<UnitOfWorkState>,
    outer: Option<Arc<InMemoryTodoUnitOfWork>>,
    on_complete: Box<
        dyn Fn(Vec<Todo>, Vec<TodoEventRecord>) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>>
            + Send
            + Sync,
    >,
}

impl InMemoryTodoUnitOfWork {
    fn new(
        outer: Option<Arc<InMemoryTodoUnitOfWork>>,
        on_complete: Box<
            dyn Fn(Vec<Todo>, Vec<TodoEventRecord>) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>>
                + Send
                + Sync,
        >,
    ) -> Self {
        Self {
            state: Mutex::new(UnitOfWorkState::default()),
            outer,
            on_complete,
        }
    }
}

#[async_trait]
impl TodoUnitOfWork for InMemoryTodoUnitOfWork {
    async fn stage_todo(&self, todo: Todo) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.todos.push(todo);
        Ok(())
    }

    async fn add_event(&self, record: TodoEventRecord) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.records.push(record);
        Ok(())
    }

    async fn complete(&self) -> Result<(), DomainError> {
        let (todos, records) = {
            let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
                message: "todo unit of work lock poisoned".to_string(),
            })?;
            (std::mem::take(&mut state.todos), std::mem::take(&mut state.records))
        };

        if let Some(outer) = &self.outer {
            for todo in todos {
                outer.stage_todo(todo).await?;
            }
            for record in records {
                outer.add_event(record).await?;
            }
            return Ok(());
        }

        (self.on_complete)(todos, records).await
    }

    async fn rollback(&self) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.todos.clear();
        state.records.clear();
        Ok(())
    }
}

type PersistTodoFn =
    dyn Fn(Todo) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>> + Send + Sync;

pub struct InMemoryTodoUnitOfWorkManager<O: TodoEventOutbox> {
    persist_todo: Arc<PersistTodoFn>,
    outbox: Arc<O>,
}

impl<O: TodoEventOutbox> InMemoryTodoUnitOfWorkManager<O> {
    pub fn new(persist_todo: Arc<PersistTodoFn>, outbox: Arc<O>) -> Self {
        Self { persist_todo, outbox }
    }
}

#[async_trait]
impl<O> TodoUnitOfWorkManager for InMemoryTodoUnitOfWorkManager<O>
where
    O: TodoEventOutbox + Send + Sync + 'static,
{
    type Uow = InMemoryTodoUnitOfWork;

    fn current(&self) -> Result<Arc<Self::Uow>, DomainError> {
        CURRENT_UOW_STACK.with(|stack| {
            stack
                .borrow()
                .last()
                .cloned()
                .ok_or_else(|| DomainError::Persistence {
                    message: "no active todo unit of work".to_string(),
                })
        })
    }

    async fn begin<F, Fut, T>(&self, op: F) -> Result<T, DomainError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, DomainError>> + Send,
        T: Send,
    {
        let outer = CURRENT_UOW_STACK.with(|stack| stack.borrow().last().cloned());
        let persist_todo = self.persist_todo.clone();
        let outbox = self.outbox.clone();
        let uow = Arc::new(InMemoryTodoUnitOfWork::new(
            outer,
            Box::new(move |todos, records| {
                let persist_todo = persist_todo.clone();
                let outbox = outbox.clone();
                Box::pin(async move {
                    for todo in todos {
                        persist_todo(todo).await?;
                    }
                    outbox.append(&records).await?;
                    Ok(())
                })
            }),
        ));

        CURRENT_UOW_STACK.with(|stack| stack.borrow_mut().push(uow.clone()));
        let result = op().await;
        CURRENT_UOW_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });

        match result {
            Ok(value) => {
                uow.complete().await?;
                Ok(value)
            }
            Err(err) => {
                let _ = uow.rollback().await;
                Err(err)
            }
        }
    }
}
