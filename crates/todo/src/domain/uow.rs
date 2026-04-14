use async_trait::async_trait;
use std::{future::Future, sync::Arc};

use crate::{
    DomainError,
    domain::{
        aggregate::Todo,
        outbox::TodoEventRecord,
    },
};

#[async_trait]
pub trait TodoUnitOfWork: Send + Sync {
    async fn stage_todo(&self, todo: Todo) -> Result<(), DomainError>;
    async fn add_event(&self, record: TodoEventRecord) -> Result<(), DomainError>;
    async fn complete(&self) -> Result<(), DomainError>;
    async fn rollback(&self) -> Result<(), DomainError>;
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
