use async_trait::async_trait;

use crate::DomainError;
use super::context::RequestContext;


#[async_trait]
pub trait CommandHandler<C>: Send + Sync {
    type Output: Send;

    async fn handle(&self, ctx: &RequestContext, Ccmd: C C) -> Result<Self::Output, DomainError>;
}

#[async_trait]
pub trait QueryHandler<Q>: Send + Sync {
    type Output: Send;

    async fn handle(&self, ctx: &RequestContext, query: Q) -> Result<Self::Output, DomainError>;
}
