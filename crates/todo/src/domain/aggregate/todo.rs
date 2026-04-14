use crate::{
    AppError,
    domain::{
        events::TodoEvent,
        model::{AggregateRoot, Entity, RecordsDomainEvents},
        value_objects::{TodoId, TodoTitle},
    },
};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Todo {
    id: TodoId,
    title: TodoTitle,
    completed: bool,
    version: u64,
    pending_events: Vec<TodoEvent>,
}

impl Todo {
    pub fn new(id: TodoId, title: TodoTitle) -> Self {
        let created_event = TodoEvent::Created {
            todo_id: id.clone(),
            title: title.clone(),
            version: 1,
            occurred_at: SystemTime::now(),
        };

        Self {
            id,
            title,
            completed: false,
            version: 1,
            pending_events: vec![created_event],
        }
    }

    pub fn complete(&mut self) -> Result<(), AppError> {
        if self.completed {
            return Err(AppError::Conflict {
                message: format!("todo {} is already completed", self.id),
            });
        }

        self.completed = true;
        self.version += 1;
        self.pending_events.push(TodoEvent::Completed {
            todo_id: self.id.clone(),
            version: self.version,
            occurred_at: SystemTime::now(),
        });

        Ok(())
    }

    pub fn id(&self) -> &TodoId {
        &self.id
    }

    pub fn title(&self) -> &TodoTitle {
        &self.title
    }

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn pull_events(&mut self) -> Vec<TodoEvent> {
        std::mem::take(&mut self.pending_events)
    }
}

impl Entity<TodoId> for Todo {
    fn id(&self) -> &TodoId {
        &self.id
    }
}

impl AggregateRoot<TodoId> for Todo {}

impl RecordsDomainEvents<TodoEvent> for Todo {
    fn pull_events(&mut self) -> Vec<TodoEvent> {
        std::mem::take(&mut self.pending_events)
    }
}

#[cfg(test)]
mod tests {
    use super::Todo;
    use crate::domain::{
        events::TodoEvent,
        model::{AggregateRoot, DomainEvent, Entity, RecordsDomainEvents, ValueObject},
        value_objects::{TodoId, TodoTitle},
    };

    fn entity_id<E>(entity: &E) -> &TodoId
    where
        E: Entity<TodoId>,
    {
        entity.id()
    }

    fn accepts_aggregate_root<A>(_aggregate: &A)
    where
        A: AggregateRoot<TodoId>,
    {
    }

    fn accepts_value_object<V>(_value: &V)
    where
        V: ValueObject,
    {
    }

    fn drain_events<A>(aggregate: &mut A) -> Vec<TodoEvent>
    where
        A: RecordsDomainEvents<TodoEvent>,
    {
        aggregate.pull_events()
    }

    fn accepts_domain_event<E>(_event: &E)
    where
        E: DomainEvent,
    {
    }

    #[test]
    fn create_todo_records_created_event() {
        let mut todo = Todo::new(
            TodoId::new("todo-1").expect("valid todo id"),
            TodoTitle::new("write docs").expect("valid todo title"),
        );

        accepts_aggregate_root(&todo);
        accepts_value_object(todo.id());
        accepts_value_object(todo.title());
        assert_eq!(todo.id().as_str(), "todo-1");
        assert_eq!(entity_id(&todo).as_str(), "todo-1");
        assert_eq!(todo.title().as_str(), "write docs");
        assert!(!todo.is_completed());
        assert_eq!(todo.version(), 1);
        let events = drain_events(&mut todo);
        assert_eq!(events.len(), 1);
        accepts_domain_event(&events[0]);
        assert_eq!(events[0].event_name(), "todo.created");
        assert_eq!(events[0].entity_id(), "todo-1");
    }

    #[test]
    fn completed_todo_cannot_be_completed_again() {
        let mut todo = Todo::new(
            TodoId::new("todo-1").expect("valid todo id"),
            TodoTitle::new("ship release").expect("valid todo title"),
        );

        todo.complete().expect("first completion succeeds");
        assert_eq!(todo.version(), 2);
        let error = todo.complete().expect_err("second completion must fail");

        assert!(error.to_string().contains("already completed"));
    }
}
