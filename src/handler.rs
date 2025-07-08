use crate::error::MqttError;
use tokio::io::{AsyncWriteExt};

#[async_trait::async_trait]
pub trait WriteStream {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), MqttError>;
    async fn flush(&mut self) -> Result<(), MqttError>;
    async fn close(&mut self, _close_frame: Option<()>) -> Result<(), MqttError>;
}

#[async_trait::async_trait]
impl WriteStream for tokio::io::WriteHalf<&mut tokio::net::TcpStream> {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), MqttError> {
        AsyncWriteExt::write_all(self, data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), MqttError> {
        AsyncWriteExt::flush(self).await?;
        Ok(())
    }

    async fn close(&mut self, _close_frame: Option<()>) -> Result<(), MqttError> {
        AsyncWriteExt::shutdown(self).await?;
        Ok(())
    }
}