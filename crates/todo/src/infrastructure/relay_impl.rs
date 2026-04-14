use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    DomainError,
    application::{TodoOutboxRelay, TodoProjectionStore},
    domain::outbox::{TodoEventInbox, TodoEventOutbox},
};

pub struct InMemoryTodoOutboxRelay<O: TodoEventOutbox, I: TodoEventInbox, P: TodoProjectionStore> {
    outbox: Arc<O>,
    inbox: Arc<I>,
    projection_store: Arc<P>,
}

impl<O: TodoEventOutbox, I: TodoEventInbox, P: TodoProjectionStore> InMemoryTodoOutboxRelay<O, I, P> {
    pub fn new(outbox: Arc<O>, inbox: Arc<I>, projection_store: Arc<P>) -> Self {
        Self {
            outbox,
            inbox,
            projection_store,
        }
    }
}

#[async_trait]
impl<O, I, P> TodoOutboxRelay for InMemoryTodoOutboxRelay<O, I, P>
where
    O: TodoEventOutbox + Send + Sync,
    I: TodoEventInbox + Send + Sync,
    P: TodoProjectionStore + Send + Sync,
{
    async fn flush(&self) -> Result<(), DomainError> {
        let pending = self.outbox.pending().await?;
        let mut published_ids = Vec::new();

        for record in pending {
            if self.inbox.contains(&record.event_id).await? {
                published_ids.push(record.event_id.clone());
                continue;
            }

            self.projection_store.apply(&record).await?;
            self.inbox.record(&record.event_id).await?;
            published_ids.push(record.event_id);
        }

        self.outbox.mark_published(&published_ids).await?;
        Ok(())
    }
}
