use std::sync::RwLock;

use async_trait::async_trait;

use crate::{
    DomainError,
    domain::outbox::{EventStatus, TodoEventOutbox, TodoEventRecord},
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
        Ok(records
            .iter()
            .filter(|record| record.status == EventStatus::Pending)
            .cloned()
            .collect())
    }

    async fn mark_published(&self, event_ids: &[String]) -> Result<(), DomainError> {
        let mut records = self.records.write().map_err(|_| DomainError::Persistence {
            message: "todo outbox write lock poisoned".to_string(),
        })?;
        for record in records.iter_mut() {
            if event_ids.iter().any(|id| id == &record.event_id) {
                record.status = EventStatus::Published;
            }
        }
        Ok(())
    }
}
