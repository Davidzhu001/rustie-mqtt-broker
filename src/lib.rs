use error::MqttError;
use state::BrokerState;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Duration;
use tracing::{debug, info, error};
use std::sync::Arc;
use bytes::Bytes;

mod client;
mod error;
mod handler;
mod v3;
mod v5;
mod state;
mod packet;
mod protocol;

pub use protocol::ProtocolVersion;

pub struct BrokerConfig {
    v3_address: Option<String>,
    v5_address: Option<String>,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
    max_connections: usize,
    max_packet_size: usize,
    connect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    disconnect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    subscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    unsubscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    publish_hook: Option<Arc<Box<dyn Fn(&str, &Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync>>>,
}

impl BrokerConfig {
    pub fn new(v3_address: Option<impl Into<String>>, v5_address: Option<impl Into<String>>) -> Self {
        BrokerConfig {
            v3_address: v3_address.map(Into::into),
            v5_address: v5_address.map(Into::into),
            auth_callback: Arc::new(Box::new(|_, _| true)),
            max_connections: 1000,
            max_packet_size: 1024 * 1024, // 1MB default
            connect_hook: None,
            disconnect_hook: None,
            subscribe_hook: None,
            unsubscribe_hook: None,
            publish_hook: None,
        }
    }

    pub fn with_auth<F>(mut self, auth_callback: F) -> Self
    where
        F: Fn(&str, &str) -> bool + Send + Sync + 'static,
    {
        self.auth_callback = Arc::new(Box::new(auth_callback));
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.max_packet_size = max_packet_size;
        self
    }

    pub fn with_connect_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.connect_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    pub fn with_disconnect_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.disconnect_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    pub fn with_subscribe_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.subscribe_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    pub fn with_unsubscribe_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.unsubscribe_hook = Some(Arc::new(Box::new(hook)));
        self
    }

    pub fn with_publish_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&str, &Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync + 'static,
    {
        self.publish_hook = Some(Arc::new(Box::new(hook)));
        self
    }
}

pub struct Broker {
    state: Arc<Mutex<BrokerState>>,
    config: BrokerConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Self {
        let (message_tx, mut message_rx) = mpsc::channel(1000);
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
                if let Err(e) = state_clone.lock().await.publish(&topic, payload, version).await {
                    error!("Failed to publish message: {}", e);
                }
            }
            info!("Message receiver loop terminated");
        });
        Broker {
            state,
            config,
            shutdown_tx: None,
        }
    }

    pub async fn start(&mut self) -> Result<(), MqttError> {
        let (tx, rx) = oneshot::channel();
        self.shutdown_tx = Some(tx);
        let rx = Arc::new(Mutex::new(Some(rx)));
        let max_connections = self.config.max_connections;
        let state = self.state.clone();
        let auth_callback = self.config.auth_callback.clone();
        let max_packet_size = self.config.max_packet_size;

        let mut tasks = Vec::new();

        if let Some(v3_address) = &self.config.v3_address {
            let listener = TcpListener::bind(v3_address).await?;
            info!("MQTT v3.1.1 Broker listening on {}", v3_address);
            let state = state.clone();
            let rx = rx.clone();
            tasks.push(tokio::spawn(async move {
                let mut current_connections = 0;
                tokio::select! {
                    _ = async {
                        let rx = rx.lock().await.take().unwrap();
                        rx.await.ok();
                    } => {
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
                                    info!("New TCP connection from {} (v3.1.1)", addr);
                                    let state = state.clone();
                                    let auth_callback = auth_callback.clone();
                                    tokio::spawn(async move {
                                        client::handle_client(stream, state, auth_callback, ProtocolVersion::V3_1_1, max_packet_size).await;
                                        current_connections -= 1;
                                    });
                                }
                                Err(e) => {
                                    error!("Error accepting TCP connection: {}", e);
                                }
                            }
                        }
                    } => {}
                }
                Ok::<(), MqttError>(())
            }));
        }

        if let Some(v5_address) = &self.config.v5_address {
            let listener = TcpListener::bind(v5_address).await?;
            info!("MQTT v5.0 Broker listening on {}", v5_address);
            let state = state.clone();
            let auth_callback = self.config.auth_callback.clone();
            tasks.push(tokio::spawn(async move {
                let mut current_connections = 0;
                tokio::select! {
                    _ = async {
                        let rx = rx.lock().await.take().unwrap();
                        rx.await.ok();
                    } => {
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
                                    info!("New TCP connection from {} (v5.0)", addr);
                                    let state = state.clone();
                                    let auth_callback = auth_callback.clone();
                                    tokio::spawn(async move {
                                        client::handle_client(stream, state, auth_callback, ProtocolVersion::V5_0, max_packet_size).await;
                                        current_connections -= 1;
                                    });
                                }
                                Err(e) => {
                                    error!("Error accepting TCP connection: {}", e);
                                }
                            }
                        }
                    } => {}
                }
                Ok::<(), MqttError>(())
            }));
        }

        futures_util::future::join_all(tasks).await.iter().for_each(|res| {
            if let Err(e) = res {
                error!("Task failed: {}", e);
            }
        });

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), MqttError> {
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).map_err(|_| MqttError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Failed to send shutdown signal")))?;
        }
        Ok(())
    }
}