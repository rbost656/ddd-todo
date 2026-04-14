use crate::domain::aggregate::Todo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoView {
    pub id: String,
    pub title: String,
    pub completed: bool,
    pub version: u64,
}

impl From<&Todo> for TodoView {
    fn from(value: &Todo) -> Self {
        Self {
            id: value.id().as_str().to_string(),
            title: value.title().as_str().to_string(),
            completed: value.is_completed(),
            version: value.version(),
        }
    }
}
