use thiserror::Error;

#[derive(Error, Debug)]
pub enum MqttError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Authentication failed")]
    AuthFailed,
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Hook error: {0}")]
    Hook(String),
}