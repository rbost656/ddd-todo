use todo::AppError;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let app = todo::bootstrap::bootstrap();

    let created = app
        .create_todo("todo-1", "build a DDD todo example")
        .await?;
    println!("created: {} - {}", created.id, created.title);

    let loaded = app.get_todo("todo-1").await?;
    println!("loaded: {} - {}", loaded.id, loaded.title);

    let completed = app.complete_todo("todo-1").await?;
    println!("completed: {} -> {}", completed.id, completed.completed);

    let todos = app.list_todos().await?;
    for todo in todos {
        println!("[{}] {} ({})", if todo.completed { "x" } else { " " }, todo.title, todo.id);
    }

    Ok(())
}
