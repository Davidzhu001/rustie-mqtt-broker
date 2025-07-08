use crate::error::MqttError;
use crate::protocol::ProtocolVersion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use futures_util::sink::SinkExt;
use bytes::Bytes;

/// Represents a TCP client connection.
pub struct Client {
    pub _id: String,
    pub stream: Arc<Mutex<TcpStream>>,
    pub subscriptions: HashSet<String>,
}

/// Represents a WebSocket client connection.
pub struct WsClient {
    pub _id: String,
    pub ws_stream: Arc<Mutex<WebSocketStream<TcpStream>>>,
    pub subscriptions: HashSet<String>,
}

/// The broker's state, managing clients and subscriptions.
pub struct BrokerState {
    clients: HashMap<String, Client>,
    ws_clients: HashMap<String, WsClient>,
    topic_subscribers: HashMap<String, HashSet<String>>,
    message_tx: mpsc::Sender<(String, Bytes, ProtocolVersion)>,
    pub connect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    pub disconnect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    pub subscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    pub unsubscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    pub publish_hook: Option<Arc<Box<dyn Fn(&str, &Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync>>>,
}

impl BrokerState {
    /// Creates a new broker state with the given message channel and hooks.
    pub fn new(
        message_tx: mpsc::Sender<(String, Bytes, ProtocolVersion)>,
        connect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
        disconnect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
        subscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
        unsubscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
        publish_hook: Option<Arc<Box<dyn Fn(&str, &Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync>>>,
    ) -> Self {
        BrokerState {
            clients: HashMap::new(),
            ws_clients: HashMap::new(),
            topic_subscribers: HashMap::new(),
            message_tx,
            connect_hook,
            disconnect_hook,
            subscribe_hook,
            unsubscribe_hook,
            publish_hook,
        }
    }

    /// Adds a TCP client to the state.
    pub fn add_client(&mut self, client_id: String, stream: Arc<Mutex<TcpStream>>) {
        let client = Client {
            _id: client_id.clone(),
            stream,
            subscriptions: HashSet::new(),
        };
        self.clients.insert(client_id.clone(), client);
    }

    /// Adds a WebSocket client to the state.
    pub fn add_ws_client(&mut self, client_id: String, ws_stream: Arc<Mutex<WebSocketStream<TcpStream>>>) {
        let client = WsClient {
            _id: client_id.clone(),
            ws_stream,
            subscriptions: HashSet::new(),
        };
        self.ws_clients.insert(client_id.clone(), client);
    }

    /// Removes a client (TCP or WebSocket) from the state.
    pub fn remove_client(&mut self, client_id: &str) {
        if self.clients.remove(client_id).is_some() || self.ws_clients.remove(client_id).is_some() {
            self.topic_subscribers.retain(|_, subs| {
                subs.remove(client_id);
                !subs.is_empty()
            });
            if let Some(hook) = self.disconnect_hook.as_ref() {
                if let Err(e) = hook(client_id) {
                    tracing::warn!("Disconnect hook failed for client {}: {}", client_id, e);
                }
            }
        }
    }

    /// Adds a subscription for a client to a topic.
    pub fn add_subscription(&mut self, client_id: &str, topic: String) {
        if self.clients.contains_key(client_id) || self.ws_clients.contains_key(client_id) {
            self.topic_subscribers
                .entry(topic.clone())
                .or_insert_with(HashSet::new)
                .insert(client_id.to_string());
            if let Some(client) = self.clients.get_mut(client_id) {
                client.subscriptions.insert(topic.clone());
            }
            if let Some(client) = self.ws_clients.get_mut(client_id) {
                client.subscriptions.insert(topic);
            }
        }
    }

    /// Removes a subscription for a client from a topic.
    pub fn remove_subscription(&mut self, client_id: &str, topic: &str) {
        if let Some(subs) = self.topic_subscribers.get_mut(topic) {
            subs.remove(client_id);
            if subs.is_empty() {
                self.topic_subscribers.remove(topic);
            }
        }
        if let Some(client) = self.clients.get_mut(client_id) {
            client.subscriptions.remove(topic);
        }
        if let Some(client) = self.ws_clients.get_mut(client_id) {
            client.subscriptions.remove(topic);
        }
    }

    /// Gets the list of subscribers for a topic.
    pub fn get_subscribers(&self, topic: &str) -> Vec<String> {
        self.topic_subscribers
            .get(topic)
            .map(|subs| subs.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Sends a message to the message channel for publishing.
    pub async fn send_message(&mut self, topic: String, payload: Bytes, version: ProtocolVersion) -> Result<(), MqttError> {
        self.message_tx.send((topic, payload, version)).await
            .map_err(|_| MqttError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Message channel closed")))?;
        Ok(())
    }

    /// Publishes a message to all subscribers of a topic.
    pub async fn publish(&mut self, topic: String, payload: Bytes, version: ProtocolVersion) -> Result<(), MqttError> {
        let subscribers = self.get_subscribers(&topic);
        let mut disconnected_clients = Vec::new();

        for sub_id in subscribers {
            if let Some(hook) = self.publish_hook.as_ref() {
                if let Err(e) = hook(&topic, &payload, version) {
                    tracing::warn!("Publish hook failed for topic {} to client {}: {}", topic, sub_id, e);
                }
            }
            if let Some(client) = self.clients.get_mut(&sub_id) {
                let mut stream = client.stream.lock().await;
                let (_, mut write_stream) = tokio::io::split(&mut *stream);
                let publish_packet = [
                    vec![0x30, (2 + topic.len() + payload.len()) as u8, 0x00, topic.len() as u8],
                    topic.as_bytes().to_vec(),
                    payload.clone().to_vec(),
                ]
                .concat();
                if let Err(e) = write_stream.write_all(&publish_packet).await {
                    tracing::error!("Failed to send PUBLISH to TCP client {}: {}", sub_id, e);
                    disconnected_clients.push(sub_id.clone());
                    continue;
                }
                if let Err(e) = write_stream.flush().await {
                    tracing::error!("Failed to flush PUBLISH to TCP client {}: {}", sub_id, e);
                    disconnected_clients.push(sub_id.clone());
                }
            }
            if let Some(client) = self.ws_clients.get_mut(&sub_id) {
                let mut ws_stream = client.ws_stream.lock().await;
                let publish_packet = [
                    vec![0x30, (2 + topic.len() + payload.len()) as u8, 0x00, topic.len() as u8],
                    topic.as_bytes().to_vec(),
                    payload.clone().to_vec(),
                ]
                .concat();
                if let Err(e) = ws_stream.send(Message::Binary(Bytes::from(publish_packet).to_vec())).await {
                    tracing::error!("Failed to send PUBLISH to WebSocket client {}: {}", sub_id, e);
                    disconnected_clients.push(sub_id.clone());
                }
            }
        }

        // Remove disconnected clients
        for client_id in disconnected_clients {
            self.remove_client(&client_id);
        }

        Ok(())
    }
}