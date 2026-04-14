#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTodo {
    pub id: String,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteTodo {
    pub id: String,
}
