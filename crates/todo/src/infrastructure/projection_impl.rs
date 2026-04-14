use std::{collections::HashMap, sync::RwLock};

use async_trait::async_trait;

use crate::{
    DomainError,
    application::{TodoProjectionStore, TodoView},
    domain::{
        events::TodoEvent,
        outbox::TodoEventRecord,
    },
};

#[derive(Default)]
pub struct InMemoryTodoProjectionStore {
    views: RwLock<HashMap<String, TodoView>>,
}

#[async_trait]
impl TodoProjectionStore for InMemoryTodoProjectionStore {
    async fn get(&self, id: &str) -> Result<Option<TodoView>, DomainError> {
        let views = self.views.read().map_err(|_| DomainError::Persistence {
            message: "todo projection read lock poisoned".to_string(),
        })?;
        Ok(views.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<TodoView>, DomainError> {
        let views = self.views.read().map_err(|_| DomainError::Persistence {
            message: "todo projection read lock poisoned".to_string(),
        })?;
        let mut items: Vec<TodoView> = views.values().cloned().collect();
        items.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(items)
    }

    async fn apply(&self, record: &TodoEventRecord) -> Result<(), DomainError> {
        let mut views = self.views.write().map_err(|_| DomainError::Persistence {
            message: "todo projection write lock poisoned".to_string(),
        })?;

        match &record.payload {
            TodoEvent::Created {
                todo_id,
                title,
                version,
                ..
            } => {
                if *version != 1 {
                    return Err(DomainError::Conflict {
                        message: format!("projection create for {} must start at version 1", todo_id),
                    });
                }

                views.insert(
                    todo_id.as_str().to_string(),
                    TodoView {
                        id: todo_id.as_str().to_string(),
                        title: title.as_str().to_string(),
                        completed: false,
                        version: *version,
                    },
                );
            }
            TodoEvent::Completed {
                todo_id, version, ..
            } => {
                let current = views.get_mut(todo_id.as_str()).ok_or_else(|| DomainError::NotFound {
                    message: format!("projection for todo {} does not exist", todo_id),
                })?;
                if current.version + 1 != *version {
                    return Err(DomainError::Conflict {
                        message: format!(
                            "projection version mismatch for todo {}: expected {}, got {}",
                            todo_id,
                            current.version + 1,
                            version
                        ),
                    });
                }

                current.completed = true;
                current.version = *version;
            }
        }

        Ok(())
    }
}
