use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Msg(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("object `{key}` failed authentication")]
    Crypto { key: String },
    #[error("object `{key}` has an unrecognized archive header")]
    Format { key: String },
    #[error("unsupported schema {schema}")]
    Schema { schema: u32 },
    #[error("catalog was updated concurrently")]
    Precondition,
    #[error(
        "local and remote copies of {harness} session {session_id} differ and neither is newer"
    )]
    Ambiguous { harness: String, session_id: String },
}

impl Error {
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Msg(message.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
