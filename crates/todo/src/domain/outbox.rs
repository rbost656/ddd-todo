use async_trait::async_trait;

use crate::{
    DomainError,
    domain::events::TodoEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStatus {
    Pending,
    Published,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoEventRecord {
    pub event_id: String,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub payload: TodoEvent,
    pub status: EventStatus,
}

impl TodoEventRecord {
    pub fn new(payload: TodoEvent) -> Self {
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
            aggregate_id,
            aggregate_version,
            payload,
            status: EventStatus::Pending,
        }
    }
}

#[async_trait]
pub trait TodoEventOutbox: Send + Sync {
    async fn append(&self, records: &[TodoEventRecord]) -> Result<(), DomainError>;
    async fn pending(&self) -> Result<Vec<TodoEventRecord>, DomainError>;
    async fn mark_published(&self, event_ids: &[String]) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TodoEventInbox: Send + Sync {
    async fn contains(&self, event_id: &str) -> Result<bool, DomainError>;
    async fn record(&self, event_id: &str) -> Result<(), DomainError>;
}
