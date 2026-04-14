use std::fmt::{Display, Formatter};

use crate::AppError;
use crate::domain::model::ValueObject;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoTitle(String);

impl TodoTitle {
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        let value = value.into().trim().to_string();
        if value.is_empty() {
            return Err(AppError::Validation {
                message: "todo title cannot be empty".to_string(),
            });
        }

        if value.len() > 120 {
            return Err(AppError::Validation {
                message: "todo title is too long".to_string(),
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for TodoTitle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl ValueObject for TodoTitle {}
