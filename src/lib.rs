mod client;
mod error;
mod handler;
mod packet;
mod protocol;
mod state;

use error::MqttError;
use state::BrokerState;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Duration;
use tracing::{debug, info};
use std::sync::Arc;

use crate::protocol::ProtocolVersion;

/// Configuration for the MQTT broker.
pub struct BrokerConfig {
    address: String,
    ws_address: Option<String>,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
    max_connections: usize,
    connect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    disconnect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    subscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    unsubscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    publish_hook: Option<Arc<Box<dyn Fn(&str, &bytes::Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync>>>,
}

impl BrokerConfig {
    /// Creates a new broker configuration with default settings.
    pub fn new(address: impl Into<String>) -> Self {
        BrokerConfig {
            address: address.into(),
            ws_address: None,
            auth_callback: Arc::new(Box::new(|username, password| {
                username == "admin" && password == "password"
            })),
            max_connections: 1000,
            connect_hook: None,
            disconnect_hook: None,
            subscribe_hook: None,
            unsubscribe_hook: None,
            publish_hook: None,
        }
    }

    /// Sets the WebSocket address for the broker.
    pub fn with_ws_address(mut self, ws_address: impl Into<String>) -> Self {
        self.ws_address = Some(ws_address.into());
        self
    }

    /// Sets the authentication callback for the broker.
    pub fn with_auth<F>(mut self, auth_callback: F) -> Self
    where
        F: Fn(&str, &str) -> bool + Send + Sync + 'static,
    {
        self.auth_callback = Arc::new(Box::new(auth_callback));
        self
    }

    /// Sets the maximum number of concurrent connections.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Sets the connect hook callback.
    pub fn with_connect_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.connect_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    /// Sets the disconnect hook callback.
    pub fn with_disconnect_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.disconnect_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    /// Sets the subscription hook callback.
    pub fn with_subscribe_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.subscribe_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    /// Sets the unsubscribe hook callback.
    pub fn with_unsubscribe_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.unsubscribe_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    /// Sets the publish hook callback.
    pub fn with_publish_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &bytes::Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync + 'static,
    {
        self.publish_hook = Some(Arc::new(Box::new(hook)));
        self
    }
}

/// The MQTT broker instance.
pub struct Broker {
    state: Arc<Mutex<BrokerState>>,
    config: BrokerConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Broker {
    /// Creates a new broker instance with the given configuration.
    pub fn new(config: BrokerConfig) -> Self {
        let (message_tx, mut message_rx) = mpsc::channel(100);
        let state = Arc::new(Mutex::new(BrokerState::new(
            message_tx,
            config.connect_hook.clone(),
            config.disconnect_hook.clone(),
            config.subscribe_hook.clone(),
            config.unsubscribe_hook.clone(),
            config.publish_hook.clone(),
        )));
        let state_clone = state.clone();
        tokio::spawn(async move {
            while let Some((topic, payload, version)) = message_rx.recv().await {
                if let Err(e) = state_clone.lock().await.publish(topic, payload, version).await {
                    tracing::error!("Failed to publish message: {}", e);
                }
            }
        });
        Broker {
            state,
            config,
            shutdown_tx: None,
        }
    }

    /// Starts the broker, listening for TCP and WebSocket connections.
    pub async fn start(&mut self) -> Result<(), MqttError> {
        let listener = TcpListener::bind(&self.config.address).await?;
        info!("Rustie MQTT Broker listening on {}", self.config.address);
        let state = self.state.clone();
        let auth_callback = self.config.auth_callback.clone();
        let max_connections = self.config.max_connections;
        let (tx, rx) = oneshot::channel();
        self.shutdown_tx = Some(tx);

        let tcp_task = tokio::spawn(async move {
            let mut current_connections = 0;
            tokio::select! {
                _ = rx => {
                    info!("Broker shutting down");
                }
                _ = async {
                    loop {
                        if current_connections >= max_connections {
                            debug!("Max connections reached: {}", max_connections);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                current_connections += 1;
                                info!("New TCP connection from {}", addr);
                                let state = state.clone();
                                let auth_callback = auth_callback.clone();
                                tokio::spawn(client::handle_client(stream, state, auth_callback));
                            }
                            Err(e) => {
                                tracing::error!("Error accepting TCP connection: {}", e);
                            }
                        }
                    }
                } => {}
            }
            Ok::<(), MqttError>(())
        });

        let ws_task = if let Some(ws_address) = self.config.ws_address.clone() {
            let ws_listener = TcpListener::bind(&ws_address).await?;
            info!("Rustie MQTT Broker WebSocket listening on {}", ws_address);
            let state = self.state.clone();
            let auth_callback = self.config.auth_callback.clone();
            Some(tokio::spawn(async move {
                loop {
                    match ws_listener.accept().await {
                        Ok((stream, addr)) => {
                            info!("New WebSocket connection from {}", addr);
                            let state = state.clone();
                            let auth_callback = auth_callback.clone();
                            tokio::spawn(client::handle_ws_client(stream, state, auth_callback));
                        }
                        Err(e) => {
                            tracing::error!("Error accepting WebSocket connection: {}", e);
                        }
                    }
                }
            }))
        } else {
            None
        };

        if let Some(ws_task) = ws_task {
            let (tcp_res, ws_res) = tokio::try_join!(tcp_task, ws_task)?;
            tcp_res?;
            let _ = ws_res;
        } else {
            tcp_task.await??;
        }

        Ok(())
    }

    /// Stops the broker gracefully.
    pub async fn stop(&mut self) -> Result<(), MqttError> {
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).map_err(|_| {
                MqttError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Failed to send shutdown signal"))
            })?;
        }
        Ok(())
    }
}