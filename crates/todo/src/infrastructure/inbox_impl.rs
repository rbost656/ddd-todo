use std::{collections::HashSet, sync::RwLock};

use async_trait::async_trait;

use crate::{
    DomainError,
    application::TodoEventInbox,
};

#[derive(Default)]
pub struct InMemoryTodoEventInbox {
    processed: RwLock<HashSet<String>>,
}

#[async_trait]
impl TodoEventInbox for InMemoryTodoEventInbox {
    async fn contains(&self, event_id: &str) -> Result<bool, DomainError> {
        let processed = self.processed.read().map_err(|_| DomainError::Persistence {
            message: "todo inbox read lock poisoned".to_string(),
        })?;
        Ok(processed.contains(event_id))
    }

    async fn record(&self, event_id: &str) -> Result<(), DomainError> {
        let mut processed = self.processed.write().map_err(|_| DomainError::Persistence {
            message: "todo inbox write lock poisoned".to_string(),
        })?;
        processed.insert(event_id.to_string());
        Ok(())
    }
}
