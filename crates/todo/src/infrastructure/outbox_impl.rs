use std::sync::RwLock;

use async_trait::async_trait;

use crate::{
    DomainError,
    application::{EventStatus, TodoEventOutbox, TodoEventRecord},
};

#[derive(Default)]
pub struct InMemoryTodoEventOutbox {
    records: RwLock<Vec<TodoEventRecord>>,
}

impl InMemoryTodoEventOutbox {
    pub fn records(&self) -> Result<Vec<TodoEventRecord>, DomainError> {
        let records = self.records.read().map_err(|_| DomainError::Persistence {
            message: "todo outbox read lock poisoned".to_string(),
        })?;
        Ok(records.clone())
    }
}

#[async_trait]
impl TodoEventOutbox for InMemoryTodoEventOutbox {
    async fn append(&self, records: &[TodoEventRecord]) -> Result<(), DomainError> {
        let mut stored = self.records.write().map_err(|_| DomainError::Persistence {
            message: "todo outbox write lock poisoned".to_string(),
        })?;
        stored.extend_from_slice(records);
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<TodoEventRecord>, DomainError> {
        let records = self.records.read().map_err(|_| DomainError::Persistence {
            message: "todo outbox read lock poisoned".to_string(),
        })?;
        let mut pending = records
            .iter()
            .filter(|record| matches!(record.status, EventStatus::Pending | EventStatus::Failed))
            .cloned()
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            left.uow_id
                .cmp(&right.uow_id)
                .then_with(|| left.event_order.cmp(&right.event_order))
        });
        Ok(pending)
    }

    async fn mark_published(&self, event_ids: &[String]) -> Result<(), DomainError> {
        let mut records = self.records.write().map_err(|_| DomainError::Persistence {
            message: "todo outbox write lock poisoned".to_string(),
        })?;
        for record in records.iter_mut() {
            if event_ids.iter().any(|id| id == &record.event_id) {
                record.status = EventStatus::Published;
                record.last_error = None;
            }
        }
        Ok(())
    }

    async fn record_failure(
        &self,
        event_id: &str,
        error_message: &str,
        max_retries: u32,
    ) -> Result<(), DomainError> {
        let mut records = self.records.write().map_err(|_| DomainError::Persistence {
            message: "todo outbox write lock poisoned".to_string(),
        })?;
        let record = records
            .iter_mut()
            .find(|record| record.event_id == event_id)
            .ok_or_else(|| DomainError::NotFound {
                message: format!("outbox record {} does not exist", event_id),
            })?;

        record.attempt_count += 1;
        record.last_error = Some(error_message.to_string());
        record.status = if record.attempt_count >= max_retries {
            EventStatus::DeadLetter
        } else {
            EventStatus::Failed
        };
        Ok(())
    }
}
