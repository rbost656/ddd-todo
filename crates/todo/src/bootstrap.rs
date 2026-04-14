use std::{collections::HashMap, sync::{Arc, RwLock}};

use crate::{
    application::{TodoProjectionService, TodoService},
    facade::TodoFacade,
    infrastructure::{
        InMemoryTodoEventInbox, InMemoryTodoEventOutbox, InMemoryTodoOutboxRelay,
        InMemoryTodoProjectionStore, InMemoryTodoRepository, InMemoryTodoUnitOfWorkManager,
    },
    domain::value_objects::TodoId,
    domain::aggregate::Todo,
    DomainError,
};

pub fn bootstrap() -> TodoFacade {
    let store = Arc::new(RwLock::new(HashMap::<TodoId, Todo>::new()));
    let outbox = Arc::new(InMemoryTodoEventOutbox::default());
    let inbox = Arc::new(InMemoryTodoEventInbox::default());
    let projection_store = Arc::new(InMemoryTodoProjectionStore::default());
    let persist_store = store.clone();
    let uow = Arc::new(InMemoryTodoUnitOfWorkManager::new(
        Arc::new(move |todo| {
            let persist_store = persist_store.clone();
            Box::pin(async move {
                let mut todos = persist_store.write().map_err(|_| DomainError::Persistence {
                    message: "todo persistent store write lock poisoned".to_string(),
                })?;
                todos.insert(todo.id().clone(), todo);
                Ok(())
            })
        }),
        outbox.clone(),
    ));
    let repo = Arc::new(InMemoryTodoRepository::new(uow.clone(), store));
    let relay = Arc::new(InMemoryTodoOutboxRelay::new(
        outbox,
        inbox,
        projection_store.clone(),
        3,
    ));
    let service = TodoService::new(repo, uow, projection_store);
    let projection_service = TodoProjectionService::new(relay);
    TodoFacade::new(service, projection_service)
}
