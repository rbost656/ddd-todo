use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum AppError {
    #[snafu(display("validation error: {message}"))]
    Validation { message: String },

    #[snafu(display("not found: {message}"))]
    NotFound { message: String },

    #[snafu(display("conflict: {message}"))]
    Conflict { message: String },

    #[snafu(display("persistence error: {message}"))]
    Persistence { message: String },
}
