use actix_ws::CloseReason;

pub type ServerResult<T> = Result<T, ServerError>;

#[derive(Clone, Debug, thiserror::Error)]
pub enum ServerError {
    #[error("")]
    CloseConnection { reason: Option<CloseReason> },
    #[error("Edge ID does not exist")]
    EdgeIdNotFound,
}