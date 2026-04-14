use async_trait::async_trait;

use crate::{
    AppError,
    domain::{
        aggregate::Todo,
        value_objects::TodoId,
    },
};

#[async_trait]
pub trait TodoRepository: Send + Sync {
    async fn save(&self, todo: Todo) -> Result<(), AppError>;
    async fn find_by_id(&self, id: &TodoId) -> Result<Option<Todo>, AppError>;
    async fn list(&self) -> Result<Vec<Todo>, AppError>;
}
