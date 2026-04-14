use async_trait::async_trait;

use crate::{
    DomainError,
    domain::events::TodoEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStatus {
    Pending,
    Failed,
    Published,
    DeadLetter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoEventRecord {
    pub event_id: String,
    pub uow_id: String,
    pub event_order: u64,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub payload: TodoEvent,
    pub attempt_count: u32,
    pub last_error: Option<String>,
    pub status: EventStatus,
}

impl TodoEventRecord {
    pub fn new(payload: TodoEvent, uow_id: impl Into<String>, event_order: u64) -> Self {
        let uow_id = uow_id.into();
        let aggregate_id = crate::DomainEvent::entity_id(&payload);
        let aggregate_version = match &payload {
            TodoEvent::Created { version, .. } | TodoEvent::Completed { version, .. } => *version,
        };
        let event_id = format!(
            "{}:{}:{}",
            aggregate_id,
            aggregate_version,
            crate::DomainEvent::event_name(&payload)
        );

        Self {
            event_id,
            uow_id,
            event_order,
            aggregate_id,
            aggregate_version,
            payload,
            attempt_count: 0,
            last_error: None,
            status: EventStatus::Pending,
        }
    }
}

#[async_trait]
pub trait TodoEventOutbox: Send + Sync {
    async fn append(&self, records: &[TodoEventRecord]) -> Result<(), DomainError>;
    async fn pending(&self) -> Result<Vec<TodoEventRecord>, DomainError>;
    async fn mark_published(&self, event_ids: &[String]) -> Result<(), DomainError>;
    async fn record_failure(
        &self,
        event_id: &str,
        error_message: &str,
        max_retries: u32,
    ) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TodoEventInbox: Send + Sync {
    async fn contains(&self, event_id: &str) -> Result<bool, DomainError>;
    async fn record(&self, event_id: &str) -> Result<(), DomainError>;
}
