use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    DomainError,
    application::{
        projection::ProjectionRunStats, TodoEventInbox, TodoEventOutbox, TodoOutboxRelay,
        TodoProjectionStore,
    },
};

pub struct InMemoryTodoOutboxRelay<O: TodoEventOutbox, I: TodoEventInbox, P: TodoProjectionStore> {
    outbox: Arc<O>,
    inbox: Arc<I>,
    projection_store: Arc<P>,
    max_retries: u32,
}

impl<O: TodoEventOutbox, I: TodoEventInbox, P: TodoProjectionStore> InMemoryTodoOutboxRelay<O, I, P> {
    pub fn new(
        outbox: Arc<O>,
        inbox: Arc<I>,
        projection_store: Arc<P>,
        max_retries: u32,
    ) -> Self {
        Self {
            outbox,
            inbox,
            projection_store,
            max_retries,
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
    async fn flush_batch(&self, limit: usize) -> Result<ProjectionRunStats, DomainError> {
        let mut stats = ProjectionRunStats::default();
        let pending = self.outbox.pending().await?;
        let pending = pending.into_iter().take(limit).collect::<Vec<_>>();
        stats.fetched = pending.len();
        let mut published_ids = Vec::new();

        for record in pending {
            if self.inbox.contains(&record.event_id).await? {
                published_ids.push(record.event_id.clone());
                stats.skipped += 1;
                continue;
            }

            match self.projection_store.apply(&record).await {
                Ok(()) => {
                    self.inbox.record(&record.event_id).await?;
                    published_ids.push(record.event_id);
                    stats.published += 1;
                }
                Err(err) => {
                    let is_dead_letter = record.attempt_count + 1 >= self.max_retries;
                    self.outbox
                        .record_failure(&record.event_id, &err.to_string(), self.max_retries)
                        .await?;
                    if is_dead_letter {
                        stats.dead_lettered += 1;
                    } else {
                        stats.failed += 1;
                    }
                }
            }
        }

        self.outbox.mark_published(&published_ids).await?;
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::SystemTime,
    };

    use async_trait::async_trait;

    use crate::{
        AppError,
        application::{TodoOutboxRelay, TodoProjectionStore, TodoView},
        domain::{
            events::TodoEvent,
            value_objects::{TodoId, TodoTitle},
        },
        application::{EventStatus, TodoEventOutbox, TodoEventRecord},
        infrastructure::{InMemoryTodoEventInbox, InMemoryTodoEventOutbox, InMemoryTodoProjectionStore},
    };

    use super::InMemoryTodoOutboxRelay;

    struct FlakyProjectionStore {
        inner: InMemoryTodoProjectionStore,
        remaining_failures: AtomicUsize,
    }

    impl FlakyProjectionStore {
        fn new(remaining_failures: usize) -> Self {
            Self {
                inner: InMemoryTodoProjectionStore::default(),
                remaining_failures: AtomicUsize::new(remaining_failures),
            }
        }
    }

    #[async_trait]
    impl TodoProjectionStore for FlakyProjectionStore {
        async fn get(&self, id: &str) -> Result<Option<TodoView>, AppError> {
            self.inner.get(id).await
        }

        async fn list(&self) -> Result<Vec<TodoView>, AppError> {
            self.inner.list().await
        }

        async fn apply(&self, record: &TodoEventRecord) -> Result<(), AppError> {
            if self.remaining_failures.load(Ordering::Relaxed) > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::Relaxed);
                return Err(AppError::Persistence {
                    message: "projection is temporarily unavailable".to_string(),
                });
            }

            self.inner.apply(record).await
        }
    }

    fn create_record() -> TodoEventRecord {
        TodoEventRecord::new(
            TodoEvent::Created {
                todo_id: TodoId::new("todo-1").expect("todo id"),
                title: TodoTitle::new("retry projection").expect("todo title"),
                version: 1,
                occurred_at: SystemTime::now(),
            },
            "uow-1",
            1,
        )
    }

    #[tokio::test]
    async fn relay_retries_failed_records_before_dead_letter() {
        let outbox = Arc::new(InMemoryTodoEventOutbox::default());
        outbox.append(&[create_record()]).await.expect("append record");
        let relay = InMemoryTodoOutboxRelay::new(
            outbox.clone(),
            Arc::new(InMemoryTodoEventInbox::default()),
            Arc::new(FlakyProjectionStore::new(1)),
            3,
        );

        let first = relay.flush().await.expect("first flush should downgrade to failed");
        assert_eq!(first.fetched, 1);
        assert_eq!(first.failed, 1);
        let after_first = outbox.records().expect("outbox records after first flush");
        assert_eq!(after_first[0].status, EventStatus::Failed);
        assert_eq!(after_first[0].attempt_count, 1);
        assert_eq!(
            after_first[0].last_error.as_deref(),
            Some("persistence error: projection is temporarily unavailable")
        );

        let second = relay.flush().await.expect("second flush should succeed");
        assert_eq!(second.fetched, 1);
        assert_eq!(second.published, 1);
        let after_second = outbox.records().expect("outbox records after second flush");
        assert_eq!(after_second[0].status, EventStatus::Published);
        assert_eq!(after_second[0].attempt_count, 1);
        assert!(after_second[0].last_error.is_none());
    }

    #[tokio::test]
    async fn relay_moves_record_to_dead_letter_after_retry_budget_is_exhausted() {
        let outbox = Arc::new(InMemoryTodoEventOutbox::default());
        outbox.append(&[create_record()]).await.expect("append record");
        let relay = InMemoryTodoOutboxRelay::new(
            outbox.clone(),
            Arc::new(InMemoryTodoEventInbox::default()),
            Arc::new(FlakyProjectionStore::new(3)),
            2,
        );

        let first = relay.flush().await.expect("first flush");
        assert_eq!(first.failed, 1);
        let second = relay.flush().await.expect("second flush");
        assert_eq!(second.dead_lettered, 1);

        let records = outbox.records().expect("outbox records");
        assert_eq!(records[0].status, EventStatus::DeadLetter);
        assert_eq!(records[0].attempt_count, 2);
        assert_eq!(
            records[0].last_error.as_deref(),
            Some("persistence error: projection is temporarily unavailable")
        );

        let pending = outbox.pending().await.expect("pending records");
        assert!(pending.is_empty());
    }
}
