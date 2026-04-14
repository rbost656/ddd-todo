use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryOrder, Schema, Set, Statement, TransactionTrait,
    entity::prelude::*,
    sea_query::OnConflict,
};
use tokio::{sync::Mutex, task_local};

use crate::{
    AppError, DomainError,
    application::{
        projection::ProjectionRunStats, CompletionHook, EventStatus, FailureHook,
        TodoEventInbox, TodoEventOutbox, TodoEventRecord, TodoOutboxRelay,
        TodoProjectionStore, TodoUnitOfWork, TodoUnitOfWorkManager, TodoView,
        UnitOfWorkCompletion, UnitOfWorkFailure,
    },
    domain::{
        aggregate::Todo,
        events::TodoEvent,
        repo::TodoRepository,
        value_objects::{TodoId, TodoTitle},
    },
};

task_local! {
    static CURRENT_SEA_ORM_UOW: Arc<SeaOrmTodoUnitOfWork>;
}

static NEXT_SEA_ORM_UOW_ID: AtomicU64 = AtomicU64::new(1);

fn persistence_error(message: impl Into<String>) -> DomainError {
    DomainError::Persistence {
        message: message.into(),
    }
}

fn system_time_to_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as i64
}

fn millis_to_system_time(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis as u64)
}

fn status_to_string(status: EventStatus) -> String {
    match status {
        EventStatus::Pending => "pending",
        EventStatus::Failed => "failed",
        EventStatus::Published => "published",
        EventStatus::DeadLetter => "dead_letter",
    }
    .to_string()
}

fn status_from_str(status: &str) -> Result<EventStatus, DomainError> {
    match status {
        "pending" => Ok(EventStatus::Pending),
        "failed" => Ok(EventStatus::Failed),
        "published" => Ok(EventStatus::Published),
        "dead_letter" => Ok(EventStatus::DeadLetter),
        other => Err(persistence_error(format!("unknown outbox status {other}"))),
    }
}

mod todo_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "todos")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub title: String,
        pub completed: bool,
        pub version: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

mod outbox_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "todo_outbox")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub event_id: String,
        pub uow_id: String,
        pub event_order: i64,
        pub aggregate_id: String,
        pub aggregate_version: i64,
        pub event_name: String,
        pub title: Option<String>,
        pub occurred_at_ms: i64,
        pub status: String,
        pub attempt_count: i32,
        pub last_error: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

mod inbox_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "todo_inbox")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub event_id: String,
        pub processed_at_ms: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

mod projection_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "todo_projection")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub title: String,
        pub completed: bool,
        pub version: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

fn todo_active_model(todo: &Todo) -> todo_entity::ActiveModel {
    todo_entity::ActiveModel {
        id: Set(todo.id().as_str().to_string()),
        title: Set(todo.title().as_str().to_string()),
        completed: Set(todo.is_completed()),
        version: Set(todo.version() as i64),
    }
}

fn todo_from_model(model: todo_entity::Model) -> Result<Todo, DomainError> {
    Ok(Todo::rehydrate(
        TodoId::new(model.id)?,
        TodoTitle::new(model.title)?,
        model.completed,
        model.version as u64,
    ))
}

fn outbox_active_model(record: &TodoEventRecord) -> outbox_entity::ActiveModel {
    let (event_name, title, occurred_at_ms) = match &record.payload {
        TodoEvent::Created {
            title, occurred_at, ..
        } => (
            "todo.created".to_string(),
            Some(title.as_str().to_string()),
            system_time_to_millis(*occurred_at),
        ),
        TodoEvent::Completed { occurred_at, .. } => (
            "todo.completed".to_string(),
            None,
            system_time_to_millis(*occurred_at),
        ),
    };

    outbox_entity::ActiveModel {
        event_id: Set(record.event_id.clone()),
        uow_id: Set(record.uow_id.clone()),
        event_order: Set(record.event_order as i64),
        aggregate_id: Set(record.aggregate_id.clone()),
        aggregate_version: Set(record.aggregate_version as i64),
        event_name: Set(event_name),
        title: Set(title),
        occurred_at_ms: Set(occurred_at_ms),
        status: Set(status_to_string(record.status)),
        attempt_count: Set(record.attempt_count as i32),
        last_error: Set(record.last_error.clone()),
    }
}

fn outbox_record_from_model(model: outbox_entity::Model) -> Result<TodoEventRecord, DomainError> {
    let payload = match model.event_name.as_str() {
        "todo.created" => TodoEvent::Created {
            todo_id: TodoId::new(model.aggregate_id.clone())?,
            title: TodoTitle::new(model.title.clone().ok_or_else(|| {
                persistence_error(format!("missing title for todo.created event {}", model.event_id))
            })?)?,
            version: model.aggregate_version as u64,
            occurred_at: millis_to_system_time(model.occurred_at_ms),
        },
        "todo.completed" => TodoEvent::Completed {
            todo_id: TodoId::new(model.aggregate_id.clone())?,
            version: model.aggregate_version as u64,
            occurred_at: millis_to_system_time(model.occurred_at_ms),
        },
        other => {
            return Err(persistence_error(format!(
                "unknown todo event name {other} for record {}",
                model.event_id
            )))
        }
    };

    Ok(TodoEventRecord {
        event_id: model.event_id,
        uow_id: model.uow_id,
        event_order: model.event_order as u64,
        aggregate_id: model.aggregate_id,
        aggregate_version: model.aggregate_version as u64,
        payload,
        attempt_count: model.attempt_count as u32,
        last_error: model.last_error,
        status: status_from_str(&model.status)?,
    })
}

fn projection_active_model(view: &TodoView) -> projection_entity::ActiveModel {
    projection_entity::ActiveModel {
        id: Set(view.id.clone()),
        title: Set(view.title.clone()),
        completed: Set(view.completed),
        version: Set(view.version as i64),
    }
}

#[derive(Default)]
struct SeaOrmUnitOfWorkState {
    staged_todos: Vec<Todo>,
    staged_events: Vec<TodoEventRecord>,
    next_event_order: u64,
    completed_hooks: Vec<CompletionHook>,
    failed_hooks: Vec<FailureHook>,
}

pub struct SeaOrmTodoRepository {
    db: DatabaseConnection,
    uow_manager: Arc<SeaOrmTodoUnitOfWorkManager>,
}

impl SeaOrmTodoRepository {
    pub fn new(db: DatabaseConnection, uow_manager: Arc<SeaOrmTodoUnitOfWorkManager>) -> Self {
        Self { db, uow_manager }
    }
}

#[async_trait]
impl TodoRepository for SeaOrmTodoRepository {
    async fn save(&self, mut todo: Todo) -> Result<(), AppError> {
        let uow = self.uow_manager.current()?;
        let events = todo.pull_events();

        let tx_guard = uow.tx.lock().await;
        let tx = tx_guard
            .as_ref()
            .ok_or_else(|| persistence_error("sea-orm unit of work transaction is no longer active"))?;

        todo_entity::Entity::insert(todo_active_model(&todo))
            .on_conflict(
                OnConflict::column(todo_entity::Column::Id)
                    .update_columns([
                        todo_entity::Column::Title,
                        todo_entity::Column::Completed,
                        todo_entity::Column::Version,
                    ])
                    .to_owned(),
            )
            .exec(tx)
            .await
            .map_err(|err| persistence_error(format!("persist todo failed: {err}")))?;
        drop(tx_guard);

        uow.stage_todo(todo).await?;
        for event in events {
            let event_order = uow.next_event_order().await?;
            let record = TodoEventRecord::new(event, uow.id().to_string(), event_order);

            let tx_guard = uow.tx.lock().await;
            let tx = tx_guard
                .as_ref()
                .ok_or_else(|| persistence_error("sea-orm unit of work transaction is no longer active"))?;
            outbox_entity::Entity::insert(outbox_active_model(&record))
                .exec(tx)
                .await
                .map_err(|err| persistence_error(format!("persist outbox record failed: {err}")))?;
            drop(tx_guard);

            uow.add_event(record).await?;
        }

        Ok(())
    }

    async fn find_by_id(&self, id: &TodoId) -> Result<Option<Todo>, AppError> {
        todo_entity::Entity::find_by_id(id.as_str().to_string())
            .one(&self.db)
            .await
            .map_err(|err| persistence_error(format!("load todo failed: {err}")))?
            .map(todo_from_model)
            .transpose()
    }

    async fn list(&self) -> Result<Vec<Todo>, AppError> {
        let models = todo_entity::Entity::find()
            .order_by_asc(todo_entity::Column::Id)
            .all(&self.db)
            .await
            .map_err(|err| persistence_error(format!("list todos failed: {err}")))?;

        models
            .into_iter()
            .map(todo_from_model)
            .collect::<Result<Vec<_>, _>>()
    }
}

pub struct SeaOrmTodoEventOutbox {
    db: DatabaseConnection,
}

impl SeaOrmTodoEventOutbox {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

pub struct SeaOrmTodoEventInbox {
    db: DatabaseConnection,
}

impl SeaOrmTodoEventInbox {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait]
impl TodoEventInbox for SeaOrmTodoEventInbox {
    async fn contains(&self, event_id: &str) -> Result<bool, DomainError> {
        let exists = inbox_entity::Entity::find_by_id(event_id.to_string())
            .one(&self.db)
            .await
            .map_err(|err| persistence_error(format!("load inbox record failed: {err}")))?;
        Ok(exists.is_some())
    }

    async fn record(&self, event_id: &str) -> Result<(), DomainError> {
        inbox_entity::Entity::insert(inbox_entity::ActiveModel {
            event_id: Set(event_id.to_string()),
            processed_at_ms: Set(system_time_to_millis(SystemTime::now())),
        })
        .on_conflict(OnConflict::column(inbox_entity::Column::EventId).do_nothing().to_owned())
        .exec(&self.db)
        .await
        .map_err(|err| persistence_error(format!("persist inbox record failed: {err}")))?;
        Ok(())
    }
}

pub struct SeaOrmTodoProjectionStore {
    db: DatabaseConnection,
}

impl SeaOrmTodoProjectionStore {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait]
impl TodoProjectionStore for SeaOrmTodoProjectionStore {
    async fn get(&self, id: &str) -> Result<Option<TodoView>, DomainError> {
        let model = projection_entity::Entity::find_by_id(id.to_string())
            .one(&self.db)
            .await
            .map_err(|err| persistence_error(format!("load projection failed: {err}")))?;
        Ok(model.map(|item| TodoView {
            id: item.id,
            title: item.title,
            completed: item.completed,
            version: item.version as u64,
        }))
    }

    async fn list(&self) -> Result<Vec<TodoView>, DomainError> {
        let models = projection_entity::Entity::find()
            .order_by_asc(projection_entity::Column::Id)
            .all(&self.db)
            .await
            .map_err(|err| persistence_error(format!("list projections failed: {err}")))?;
        Ok(models
            .into_iter()
            .map(|item| TodoView {
                id: item.id,
                title: item.title,
                completed: item.completed,
                version: item.version as u64,
            })
            .collect())
    }

    async fn apply(&self, record: &TodoEventRecord) -> Result<(), DomainError> {
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

                projection_entity::Entity::insert(projection_active_model(&TodoView {
                    id: todo_id.as_str().to_string(),
                    title: title.as_str().to_string(),
                    completed: false,
                    version: *version,
                }))
                .on_conflict(OnConflict::column(projection_entity::Column::Id).do_nothing().to_owned())
                .exec(&self.db)
                .await
                .map_err(|err| persistence_error(format!("persist projection create failed: {err}")))?;
                Ok(())
            }
            TodoEvent::Completed {
                todo_id, version, ..
            } => {
                let model = projection_entity::Entity::find_by_id(todo_id.as_str().to_string())
                    .one(&self.db)
                    .await
                    .map_err(|err| persistence_error(format!("load projection for update failed: {err}")))?
                    .ok_or_else(|| DomainError::NotFound {
                        message: format!("projection for todo {} does not exist", todo_id),
                    })?;

                if model.version as u64 + 1 != *version {
                    return Err(DomainError::Conflict {
                        message: format!(
                            "projection version mismatch for todo {}: expected {}, got {}",
                            todo_id,
                            model.version + 1,
                            version
                        ),
                    });
                }

                let mut active_model: projection_entity::ActiveModel = model.into();
                active_model.completed = Set(true);
                active_model.version = Set(*version as i64);
                active_model
                    .update(&self.db)
                    .await
                    .map_err(|err| persistence_error(format!("persist projection update failed: {err}")))?;
                Ok(())
            }
        }
    }
}

pub struct SeaOrmTodoProjectionWorker<R: TodoOutboxRelay> {
    relay: Arc<R>,
}

impl<R: TodoOutboxRelay> SeaOrmTodoProjectionWorker<R> {
    pub fn new(relay: Arc<R>) -> Self {
        Self { relay }
    }

    pub async fn run_once(&self) -> Result<ProjectionRunStats, DomainError> {
        self.relay.flush().await
    }

    pub async fn run_batch(&self, limit: usize) -> Result<ProjectionRunStats, DomainError> {
        self.relay.flush_batch(limit).await
    }

    pub async fn drain(&self, batch_size: usize, max_batches: usize) -> Result<ProjectionRunStats, DomainError> {
        let mut total = ProjectionRunStats::default();

        for _ in 0..max_batches {
            let batch: ProjectionRunStats = self.run_batch(batch_size).await?;
            let is_empty = batch.is_empty();
            total.merge(batch);
            if is_empty {
                break;
            }
        }

        Ok(total)
    }
}

#[async_trait]
impl TodoEventOutbox for SeaOrmTodoEventOutbox {
    async fn append(&self, records: &[TodoEventRecord]) -> Result<(), DomainError> {
        for record in records {
            outbox_entity::Entity::insert(outbox_active_model(record))
                .exec(&self.db)
                .await
                .map_err(|err| persistence_error(format!("append outbox record failed: {err}")))?;
        }
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<TodoEventRecord>, DomainError> {
        let models = outbox_entity::Entity::find()
            .filter(
                outbox_entity::Column::Status
                    .is_in([status_to_string(EventStatus::Pending), status_to_string(EventStatus::Failed)]),
            )
            .order_by_asc(outbox_entity::Column::UowId)
            .order_by_asc(outbox_entity::Column::EventOrder)
            .all(&self.db)
            .await
            .map_err(|err| persistence_error(format!("load pending outbox failed: {err}")))?;

        models
            .into_iter()
            .map(outbox_record_from_model)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn mark_published(&self, event_ids: &[String]) -> Result<(), DomainError> {
        for event_id in event_ids {
            if let Some(model) = outbox_entity::Entity::find_by_id(event_id.clone())
                .one(&self.db)
                .await
                .map_err(|err| persistence_error(format!("load outbox record failed: {err}")))? {
                let mut active_model: outbox_entity::ActiveModel = model.into();
                active_model.status = Set(status_to_string(EventStatus::Published));
                active_model.last_error = Set(None);
                active_model
                    .update(&self.db)
                    .await
                    .map_err(|err| persistence_error(format!("mark published failed: {err}")))?;
            }
        }
        Ok(())
    }

    async fn record_failure(
        &self,
        event_id: &str,
        error_message: &str,
        max_retries: u32,
    ) -> Result<(), DomainError> {
        let model = outbox_entity::Entity::find_by_id(event_id.to_string())
            .one(&self.db)
            .await
            .map_err(|err| persistence_error(format!("load outbox record failed: {err}")))?
            .ok_or_else(|| persistence_error(format!("outbox record {event_id} does not exist")))?;

        let next_attempt_count = model.attempt_count + 1;
        let next_status = if next_attempt_count >= max_retries as i32 {
            EventStatus::DeadLetter
        } else {
            EventStatus::Failed
        };

        let mut active_model: outbox_entity::ActiveModel = model.into();
        active_model.attempt_count = Set(next_attempt_count);
        active_model.last_error = Set(Some(error_message.to_string()));
        active_model.status = Set(status_to_string(next_status));
        active_model
            .update(&self.db)
            .await
            .map_err(|err| persistence_error(format!("update outbox failure state failed: {err}")))?;
        Ok(())
    }
}

pub struct SeaOrmTodoUnitOfWork {
    id: String,
    tx: Mutex<Option<DatabaseTransaction>>,
    state: Mutex<SeaOrmUnitOfWorkState>,
}

impl SeaOrmTodoUnitOfWork {
    fn new(id: String, tx: DatabaseTransaction) -> Self {
        Self {
            id,
            tx: Mutex::new(Some(tx)),
            state: Mutex::new(SeaOrmUnitOfWorkState::default()),
        }
    }

    async fn take_tx(&self) -> Result<DatabaseTransaction, DomainError> {
        self.tx
            .lock()
            .await
            .take()
            .ok_or_else(|| persistence_error(format!("sea-orm unit of work {} has no active transaction", self.id)))
    }
}

#[async_trait]
impl TodoUnitOfWork for SeaOrmTodoUnitOfWork {
    fn id(&self) -> &str {
        &self.id
    }

    async fn next_event_order(&self) -> Result<u64, DomainError> {
        let mut state = self.state.lock().await;
        state.next_event_order += 1;
        Ok(state.next_event_order)
    }

    async fn stage_todo(&self, todo: Todo) -> Result<(), DomainError> {
        let mut state = self.state.lock().await;
        state.staged_todos.push(todo);
        Ok(())
    }

    async fn add_event(&self, record: TodoEventRecord) -> Result<(), DomainError> {
        let mut state = self.state.lock().await;
        state.staged_events.push(record);
        Ok(())
    }

    async fn on_completed(&self, hook: CompletionHook) -> Result<(), DomainError> {
        let mut state = self.state.lock().await;
        state.completed_hooks.push(hook);
        Ok(())
    }

    async fn on_failed(&self, hook: FailureHook) -> Result<(), DomainError> {
        let mut state = self.state.lock().await;
        state.failed_hooks.push(hook);
        Ok(())
    }

    async fn complete(&self) -> Result<(), DomainError> {
        let (persisted_todos, persisted_event_ids, hooks) = {
            let state = self.state.lock().await;
            (
                state.staged_todos.len(),
                state
                    .staged_events
                    .iter()
                    .map(|record| record.event_id.clone())
                    .collect::<Vec<_>>(),
                state.completed_hooks.clone(),
            )
        };

        let tx = self.take_tx().await?;
        tx.commit()
            .await
            .map_err(|err| persistence_error(format!("sea-orm transaction commit failed: {err}")))?;

        let completion = UnitOfWorkCompletion {
            uow_id: self.id.clone(),
            persisted_todos,
            persisted_event_ids,
        };
        for hook in hooks {
            hook(completion.clone()).await?;
        }
        Ok(())
    }

    async fn rollback(&self, reason: String) -> Result<(), DomainError> {
        let (staged_todos, staged_events, hooks) = {
            let state = self.state.lock().await;
            (
                state.staged_todos.len(),
                state.staged_events.len(),
                state.failed_hooks.clone(),
            )
        };

        let tx = self.take_tx().await?;
        tx.rollback()
            .await
            .map_err(|err| persistence_error(format!("sea-orm transaction rollback failed: {err}")))?;

        let failure = UnitOfWorkFailure {
            uow_id: self.id.clone(),
            staged_todos,
            staged_events,
            reason,
        };
        for hook in hooks {
            hook(failure.clone()).await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SeaOrmTodoUnitOfWorkManager {
    db: DatabaseConnection,
}

impl SeaOrmTodoUnitOfWorkManager {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    pub async fn ensure_schema(&self) -> Result<(), DomainError> {
        let backend = self.db.get_database_backend();
        let schema = Schema::new(backend);

        for statement in [
            schema.create_table_from_entity(todo_entity::Entity).if_not_exists().to_owned(),
            schema.create_table_from_entity(outbox_entity::Entity).if_not_exists().to_owned(),
            schema.create_table_from_entity(inbox_entity::Entity).if_not_exists().to_owned(),
            schema.create_table_from_entity(projection_entity::Entity).if_not_exists().to_owned(),
        ] {
            let sql = backend.build(&statement).to_string();
            self.db
                .execute(Statement::from_string(backend, sql))
                .await
                .map_err(|err| persistence_error(format!("create schema failed: {err}")))?;
        }

        Ok(())
    }
}

#[async_trait]
impl TodoUnitOfWorkManager for SeaOrmTodoUnitOfWorkManager {
    type Uow = SeaOrmTodoUnitOfWork;

    fn current(&self) -> Result<Arc<Self::Uow>, DomainError> {
        CURRENT_SEA_ORM_UOW
            .try_with(Arc::clone)
            .map_err(|_| persistence_error("no active sea-orm unit of work in current task"))
    }

    async fn begin<F, Fut, T>(&self, op: F) -> Result<T, DomainError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, DomainError>> + Send,
        T: Send,
    {
        if CURRENT_SEA_ORM_UOW.try_with(|_| ()).is_ok() {
            return op().await;
        }

        let tx = self
            .db
            .begin()
            .await
            .map_err(|err| persistence_error(format!("sea-orm transaction begin failed: {err}")))?;
        let uow = Arc::new(SeaOrmTodoUnitOfWork::new(
            format!("sea-uow-{}", NEXT_SEA_ORM_UOW_ID.fetch_add(1, Ordering::Relaxed)),
            tx,
        ));

        let result = CURRENT_SEA_ORM_UOW.scope(uow.clone(), async move { op().await }).await;
        match result {
            Ok(value) => {
                uow.complete().await?;
                Ok(value)
            }
            Err(err) => {
                let _ = uow.rollback(err.to_string()).await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::Database;

    use super::{
        SeaOrmTodoEventInbox, SeaOrmTodoEventOutbox, SeaOrmTodoProjectionStore,
        SeaOrmTodoProjectionWorker, SeaOrmTodoRepository, SeaOrmTodoUnitOfWorkManager,
    };
    use crate::{
        application::TodoProjectionStore,
        domain::{
            builders::TodoBuilder,
            model::Builder,
            repo::TodoRepository,
            value_objects::TodoId,
        },
        application::{EventStatus, TodoEventInbox, TodoEventOutbox, TodoUnitOfWorkManager},
        infrastructure::InMemoryTodoOutboxRelay,
        DomainError,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn sea_orm_repository_persists_todos_and_outbox_records() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        let manager = Arc::new(SeaOrmTodoUnitOfWorkManager::new(db.clone()));
        manager.ensure_schema().await.expect("ensure schema");
        let repo = SeaOrmTodoRepository::new(db.clone(), manager.clone());
        let outbox = SeaOrmTodoEventOutbox::new(db);
        let builder = TodoBuilder;

        manager
            .begin(|| async {
                let todo = builder.build(crate::application::CreateTodo {
                    id: "todo-1".to_string(),
                    title: "persist with sea-orm".to_string(),
                })?;
                repo.save(todo).await?;
                Ok(())
            })
            .await
            .expect("commit todo");

        let loaded = repo
            .find_by_id(&TodoId::new("todo-1").expect("todo id"))
            .await
            .expect("load todo")
            .expect("todo exists");
        assert_eq!(loaded.id().as_str(), "todo-1");
        assert_eq!(loaded.title().as_str(), "persist with sea-orm");
        assert_eq!(loaded.version(), 1);

        let records = outbox.pending().await.expect("pending outbox");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, EventStatus::Pending);
        assert_eq!(records[0].aggregate_id, "todo-1");
        assert_eq!(records[0].event_order, 1);
    }

    #[tokio::test]
    async fn sea_orm_outbox_transitions_failure_state() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        let manager = Arc::new(SeaOrmTodoUnitOfWorkManager::new(db.clone()));
        manager.ensure_schema().await.expect("ensure schema");
        let repo = SeaOrmTodoRepository::new(db.clone(), manager.clone());
        let outbox = SeaOrmTodoEventOutbox::new(db);
        let builder = TodoBuilder;

        manager
            .begin(|| async {
                let mut todo = builder.build(crate::application::CreateTodo {
                    id: "todo-2".to_string(),
                    title: "retry with sea-orm".to_string(),
                })?;
                todo.complete()?;
                repo.save(todo).await?;
                Ok(())
            })
            .await
            .expect("commit todo");

        let pending = outbox.pending().await.expect("pending outbox");
        assert_eq!(pending.len(), 2);

        outbox
            .record_failure(&pending[0].event_id, "projection failed", 2)
            .await
            .expect("record failure");
        let failed = outbox.pending().await.expect("pending after failure");
        assert_eq!(failed[0].status, EventStatus::Failed);
        assert_eq!(failed[0].attempt_count, 1);

        outbox
            .record_failure(&failed[0].event_id, "projection failed again", 2)
            .await
            .expect("record second failure");
        let pending_after_dead_letter = outbox.pending().await.expect("pending after dlq");
        assert_eq!(pending_after_dead_letter.len(), 1);
        assert_eq!(pending_after_dead_letter[0].event_order, 2);

        let all_pending_ids = pending_after_dead_letter
            .iter()
            .map(|record| record.event_id.clone())
            .collect::<Vec<_>>();
        assert!(!all_pending_ids.iter().any(|id| id.contains(":1:todo.created")));
    }

    #[tokio::test]
    async fn sea_orm_uow_rolls_back_on_error() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        let manager = Arc::new(SeaOrmTodoUnitOfWorkManager::new(db.clone()));
        manager.ensure_schema().await.expect("ensure schema");
        let repo = SeaOrmTodoRepository::new(db.clone(), manager.clone());
        let builder = TodoBuilder;

        let result: Result<(), DomainError> = manager
            .begin(|| async {
                let todo = builder.build(crate::application::CreateTodo {
                    id: "todo-rollback".to_string(),
                    title: "rollback".to_string(),
                })?;
                repo.save(todo).await?;
                Err(DomainError::Validation {
                    message: "force rollback".to_string(),
                })
            })
            .await;
        assert!(matches!(result, Err(DomainError::Validation { .. })));

        let loaded = repo
            .find_by_id(&TodoId::new("todo-rollback").expect("todo id"))
            .await
            .expect("load todo after rollback");
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn sea_orm_projection_pipeline_updates_read_model_and_inbox() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        let manager = Arc::new(SeaOrmTodoUnitOfWorkManager::new(db.clone()));
        manager.ensure_schema().await.expect("ensure schema");
        let repo = SeaOrmTodoRepository::new(db.clone(), manager.clone());
        let outbox = Arc::new(SeaOrmTodoEventOutbox::new(db.clone()));
        let inbox = Arc::new(SeaOrmTodoEventInbox::new(db.clone()));
        let projection_store = Arc::new(SeaOrmTodoProjectionStore::new(db));
        let relay = Arc::new(InMemoryTodoOutboxRelay::new(
            outbox.clone(),
            inbox.clone(),
            projection_store.clone(),
            3,
        ));
        let worker = SeaOrmTodoProjectionWorker::new(relay);
        let builder = TodoBuilder;

        manager
            .begin(|| async {
                let mut todo = builder.build(crate::application::CreateTodo {
                    id: "todo-projection".to_string(),
                    title: "project me".to_string(),
                })?;
                todo.complete()?;
                repo.save(todo).await?;
                Ok(())
            })
            .await
            .expect("commit todo");

        let stats = worker.run_once().await.expect("run projection worker");
        assert_eq!(stats.fetched, 2);
        assert_eq!(stats.published, 2);

        let projection = projection_store
            .get("todo-projection")
            .await
            .expect("load projection")
            .expect("projection exists");
        assert_eq!(projection.id, "todo-projection");
        assert_eq!(projection.title, "project me");
        assert!(projection.completed);
        assert_eq!(projection.version, 2);

        assert!(inbox
            .contains("todo-projection:1:todo.created")
            .await
            .expect("created inbox record"));
        assert!(inbox
            .contains("todo-projection:2:todo.completed")
            .await
            .expect("completed inbox record"));

        let remaining = outbox.pending().await.expect("pending after projection");
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn sea_orm_projection_worker_drains_in_batches() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite memory");
        let manager = Arc::new(SeaOrmTodoUnitOfWorkManager::new(db.clone()));
        manager.ensure_schema().await.expect("ensure schema");
        let repo = SeaOrmTodoRepository::new(db.clone(), manager.clone());
        let outbox = Arc::new(SeaOrmTodoEventOutbox::new(db.clone()));
        let projection_store = Arc::new(SeaOrmTodoProjectionStore::new(db));
        let relay = Arc::new(InMemoryTodoOutboxRelay::new(
            outbox.clone(),
            Arc::new(SeaOrmTodoEventInbox::new(manager.db().clone())),
            projection_store,
            3,
        ));
        let worker = SeaOrmTodoProjectionWorker::new(relay);
        let builder = TodoBuilder;

        for id in ["todo-a", "todo-b", "todo-c"] {
            manager
                .begin(|| async {
                    let todo = builder.build(crate::application::CreateTodo {
                        id: id.to_string(),
                        title: format!("title-{id}"),
                    })?;
                    repo.save(todo).await?;
                    Ok(())
                })
                .await
                .expect("commit todo");
        }

        let stats = worker.drain(2, 5).await.expect("drain worker");
        assert_eq!(stats.fetched, 3);
        assert_eq!(stats.published, 3);

        let remaining = outbox.pending().await.expect("pending after drain");
        assert!(remaining.is_empty());
    }
}
