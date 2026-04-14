use std::time::SystemTime;

pub trait Entity<Key>: Send + Sync
where
    Key: Send + Sync,
{
    fn id(&self) -> &Key;
}

pub trait AggregateRoot<Key>: Entity<Key>
where
    Key: Send + Sync,
{
}

pub trait ValueObject: Clone + Eq + Send + Sync + 'static {}

pub trait DomainEvent: Clone + Send + Sync + 'static {
    fn event_name(&self) -> &'static str;
    fn entity_id(&self) -> String;
    fn occurred_at(&self) -> SystemTime;
}

pub trait RecordsDomainEvents<Event>: Send + Sync
where
    Event: DomainEvent,
{
    fn pull_events(&mut self) -> Vec<Event>;
}

pub trait DomainEventPublisher<Event>: Send + Sync
where
    Event: DomainEvent,
{
    fn publish(&self, events: &[Event]) -> Result<(), crate::DomainError>;
}

pub trait Builder: Send + Sync {
    type Input;
    type Output;

    fn build(&self, input: Self::Input) -> Result<Self::Output, crate::DomainError>;
}
