use std::sync::Arc;

use crate::{
    DomainError,
    application::projection::TodoOutboxRelay,
};

pub struct TodoProjectionService<L: TodoOutboxRelay> {
    relay: Arc<L>,
}

impl<L: TodoOutboxRelay> TodoProjectionService<L> {
    pub fn new(relay: Arc<L>) -> Self {
        Self { relay }
    }

    pub async fn flush_outbox(&self) -> Result<(), DomainError> {
        self.relay.flush().await
    }
}
