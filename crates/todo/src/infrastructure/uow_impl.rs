use std::{
    cell::RefCell,
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;

use crate::{
    DomainError,
    application::{
        CompletionHook, FailureHook, TodoEventOutbox, TodoEventRecord, TodoUnitOfWork,
        TodoUnitOfWorkManager, UnitOfWorkCompletion, UnitOfWorkFailure,
    },
    domain::{
        aggregate::Todo,
    },
};

thread_local! {
    static CURRENT_UOW_STACK: RefCell<Vec<Arc<InMemoryTodoUnitOfWork>>> = const { RefCell::new(Vec::new()) };
}

static NEXT_UOW_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct UnitOfWorkState {
    todos: Vec<Todo>,
    records: Vec<TodoEventRecord>,
    next_event_order: u64,
    completed_hooks: Vec<CompletionHook>,
    failed_hooks: Vec<FailureHook>,
}

pub struct InMemoryTodoUnitOfWork {
    id: String,
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
        id: String,
        outer: Option<Arc<InMemoryTodoUnitOfWork>>,
        on_complete: Box<
            dyn Fn(Vec<Todo>, Vec<TodoEventRecord>) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>>
                + Send
                + Sync,
        >,
    ) -> Self {
        Self {
            id,
            state: Mutex::new(UnitOfWorkState::default()),
            outer,
            on_complete,
        }
    }
}

#[async_trait]
impl TodoUnitOfWork for InMemoryTodoUnitOfWork {
    fn id(&self) -> &str {
        &self.id
    }

    async fn next_event_order(&self) -> Result<u64, DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.next_event_order += 1;
        Ok(state.next_event_order)
    }

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

    async fn on_completed(&self, hook: CompletionHook) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.completed_hooks.push(hook);
        Ok(())
    }

    async fn on_failed(&self, hook: FailureHook) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
            message: "todo unit of work lock poisoned".to_string(),
        })?;
        state.failed_hooks.push(hook);
        Ok(())
    }

    async fn complete(&self) -> Result<(), DomainError> {
        let (todos, records, next_event_order, completed_hooks, failed_hooks) = {
            let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
                message: "todo unit of work lock poisoned".to_string(),
            })?;
            (
                std::mem::take(&mut state.todos),
                std::mem::take(&mut state.records),
                state.next_event_order,
                std::mem::take(&mut state.completed_hooks),
                std::mem::take(&mut state.failed_hooks),
            )
        };

        if let Some(outer) = &self.outer {
            {
                let mut outer_state = outer.state.lock().map_err(|_| DomainError::Persistence {
                    message: "todo unit of work lock poisoned".to_string(),
                })?;
                outer_state.next_event_order = outer_state.next_event_order.max(next_event_order);
                outer_state.completed_hooks.extend(completed_hooks);
                outer_state.failed_hooks.extend(failed_hooks);
            }

            for todo in todos {
                outer.stage_todo(todo).await?;
            }
            for record in records {
                outer.add_event(record).await?;
            }
            return Ok(());
        }

        let persisted_todos = dedupe_todos(todos);
        let persisted_event_ids = records
            .iter()
            .map(|record| record.event_id.clone())
            .collect::<Vec<_>>();
        let sorted_records = sort_records(records);
        (self.on_complete)(persisted_todos.clone(), sorted_records).await?;

        let completion = UnitOfWorkCompletion {
            uow_id: self.id.clone(),
            persisted_todos: persisted_todos.len(),
            persisted_event_ids,
        };
        for hook in completed_hooks {
            hook(completion.clone()).await?;
        }
        Ok(())
    }

    async fn rollback(&self, reason: String) -> Result<(), DomainError> {
        let (staged_todos, staged_events, failed_hooks) = {
            let mut state = self.state.lock().map_err(|_| DomainError::Persistence {
                message: "todo unit of work lock poisoned".to_string(),
            })?;
            let staged_todos = state.todos.len();
            let staged_events = state.records.len();
            state.todos.clear();
            state.records.clear();
            state.next_event_order = 0;
            state.completed_hooks.clear();
            let failed_hooks = std::mem::take(&mut state.failed_hooks);
            (staged_todos, staged_events, failed_hooks)
        };

        let failure = UnitOfWorkFailure {
            uow_id: self.id.clone(),
            staged_todos,
            staged_events,
            reason,
        };
        for hook in failed_hooks {
            hook(failure.clone()).await?;
        }
        Ok(())
    }
}

fn dedupe_todos(todos: Vec<Todo>) -> Vec<Todo> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(todos.len());

    for todo in todos.into_iter().rev() {
        let todo_id = todo.id().clone();
        if seen.insert(todo_id) {
            deduped.push(todo);
        }
    }

    deduped.reverse();
    deduped
}

fn sort_records(mut records: Vec<TodoEventRecord>) -> Vec<TodoEventRecord> {
    records.sort_by(|left, right| left.event_order.cmp(&right.event_order));
    records
}

fn next_uow_id() -> String {
    format!("uow-{}", NEXT_UOW_ID.fetch_add(1, Ordering::Relaxed))
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
            next_uow_id(),
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
                let _ = uow.rollback(err.to_string()).await;
                Err(err)
            }
        }
    }
}
