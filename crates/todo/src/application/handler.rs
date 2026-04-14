use async_trait::async_trait;

use crate::DomainError;

#[async_trait]
pub trait CommandHandler<C>: Send + Sync {
    type Output: Send;

    async fn handle(&self, cmd: C) -> Result<Self::Output, DomainError>;
}

#[async_trait]
pub trait QueryHandler<Q>: Send + Sync {
    type Output: Send;

    async fn handle(&self, query: Q) -> Result<Self::Output, DomainError>;
}
