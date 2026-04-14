use std::time::SystemTime;

use crate::domain::model::DomainEvent;
use crate::domain::value_objects::{TodoId, TodoTitle};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TodoEvent {
    Created {
        todo_id: TodoId,
        title: TodoTitle,
        version: u64,
        occurred_at: SystemTime,
    },
    Completed {
        todo_id: TodoId,
        version: u64,
        occurred_at: SystemTime,
    },
}

impl DomainEvent for TodoEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::Created { .. } => "todo.created",
            Self::Completed { .. } => "todo.completed",
        }
    }

    fn entity_id(&self) -> String {
        match self {
            Self::Created { todo_id, .. } | Self::Completed { todo_id, .. } => todo_id.as_str().to_string(),
        }
    }

    fn occurred_at(&self) -> SystemTime {
        match self {
            Self::Created { occurred_at, .. } | Self::Completed { occurred_at, .. } => *occurred_at,
        }
    }
}
