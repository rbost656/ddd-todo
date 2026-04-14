pub mod application;
pub mod bootstrap;
pub mod domain;
pub mod facade;
pub mod infrastructure;

pub use domain::model::{
    AggregateRoot, Builder, DomainEvent, DomainEventPublisher, Entity, RecordsDomainEvents,
    ValueObject,
};
pub use domain::errors::AppError;
pub type DomainError = AppError;
pub use facade::TodoFacade;
