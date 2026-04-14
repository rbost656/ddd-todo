use async_trait::async_trait;

use crate::{
    DomainError,
    application::dto::TodoView,
    application::outbox::TodoEventRecord,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ProjectionRunStats {
    pub fetched: usize,
    pub published: usize,
    pub skipped: usize,
    pub failed: usize,
    pub dead_lettered: usize,
}

impl ProjectionRunStats {
    pub fn is_empty(&self) -> bool {
        self.fetched == 0
    }

    pub fn merge(&mut self, other: Self) {
        self.fetched += other.fetched;
        self.published += other.published;
        self.skipped += other.skipped;
        self.failed += other.failed;
        self.dead_lettered += other.dead_lettered;
    }
}

#[async_trait]
pub trait TodoProjectionStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<TodoView>, DomainError>;
    async fn list(&self) -> Result<Vec<TodoView>, DomainError>;
    async fn apply(&self, record: &TodoEventRecord) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TodoOutboxRelay: Send + Sync {
    async fn flush_batch(&self, limit: usize) -> Result<ProjectionRunStats, DomainError>;

    async fn flush(&self) -> Result<ProjectionRunStats, DomainError> {
        self.flush_batch(usize::MAX).await
    }
}
