use crate::{
    DomainError,
    application::dto::CreateTodo,
    domain::{
        aggregate::Todo,
        model::Builder,
        value_objects::{TodoId, TodoTitle},
    },
};

pub struct TodoBuilder;

impl Builder for TodoBuilder {
    type Input = CreateTodo;
    type Output = Todo;

    fn build(&self, input: Self::Input) -> Result<Self::Output, DomainError> {
        let id = TodoId::new(input.id)?;
        let title = TodoTitle::new(input.title)?;
        Ok(Todo::new(id, title))
    }
}
