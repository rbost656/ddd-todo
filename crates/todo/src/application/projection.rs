use async_trait::async_trait;

use crate::{
    DomainError,
    application::dto::TodoView,
    domain::outbox::TodoEventRecord,
};

#[async_trait]
pub trait TodoProjectionStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<TodoView>, DomainError>;
    async fn list(&self) -> Result<Vec<TodoView>, DomainError>;
    async fn apply(&self, record: &TodoEventRecord) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TodoOutboxRelay: Send + Sync {
    async fn flush(&self) -> Result<(), DomainError>;
}
