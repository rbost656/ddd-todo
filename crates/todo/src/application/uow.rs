use async_trait::async_trait;
use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    DomainError,
    application::outbox::TodoEventRecord,
    domain::aggregate::Todo,
};

pub type CompletionHook = Arc<
    dyn Fn(UnitOfWorkCompletion) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>>
        + Send
        + Sync,
>;

pub type FailureHook = Arc<
    dyn Fn(UnitOfWorkFailure) -> Pin<Box<dyn Future<Output = Result<(), DomainError>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone)]
pub struct UnitOfWorkCompletion {
    pub uow_id: String,
    pub persisted_todos: usize,
    pub persisted_event_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UnitOfWorkFailure {
    pub uow_id: String,
    pub staged_todos: usize,
    pub staged_events: usize,
    pub reason: String,
}

#[async_trait]
pub trait TodoUnitOfWork: Send + Sync {
    fn id(&self) -> &str;
    async fn next_event_order(&self) -> Result<u64, DomainError>;
    async fn stage_todo(&self, todo: Todo) -> Result<(), DomainError>;
    async fn add_event(&self, record: TodoEventRecord) -> Result<(), DomainError>;
    async fn on_completed(&self, hook: CompletionHook) -> Result<(), DomainError>;
    async fn on_failed(&self, hook: FailureHook) -> Result<(), DomainError>;
    async fn complete(&self) -> Result<(), DomainError>;
    async fn rollback(&self, reason: String) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TodoUnitOfWorkManager: Send + Sync {
    type Uow: TodoUnitOfWork;

    fn current(&self) -> Result<Arc<Self::Uow>, DomainError>;
    async fn begin<F, Fut, T>(&self, op: F) -> Result<T, DomainError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, DomainError>> + Send,
        T: Send;
}
