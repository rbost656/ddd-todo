use std::sync::Arc;

use crate::{
    DomainError,
    application::projection::{ProjectionRunStats, TodoOutboxRelay},
};

pub struct TodoProjectionService<L: TodoOutboxRelay> {
    relay: Arc<L>,
}

impl<L: TodoOutboxRelay> TodoProjectionService<L> {
    pub fn new(relay: Arc<L>) -> Self {
        Self { relay }
    }

    pub async fn flush_outbox(&self) -> Result<ProjectionRunStats, DomainError> {
        self.relay.flush().await
    }

    pub async fn flush_outbox_batch(&self, limit: usize) -> Result<ProjectionRunStats, DomainError> {
        self.relay.flush_batch(limit).await
    }
}
