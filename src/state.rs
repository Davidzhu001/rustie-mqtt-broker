use crate::error::MqttError;
use crate::protocol::ProtocolVersion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use bytes::Bytes;
use tracing::{error};

pub struct Client {
    pub _id: String,
    pub stream: Arc<Mutex<TcpStream>>,
    pub subscriptions: HashSet<String>,
}

pub struct BrokerState {
    clients: HashMap<String, Client>,
    topic_subscribers: HashMap<String, HashSet<String>>,
    retained_messages: HashMap<String, (Bytes, ProtocolVersion)>,
    shared_subscribers: HashMap<String, Vec<String>>,
    pub message_tx: mpsc::Sender<(String, Bytes, ProtocolVersion)>,
    pub connect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    pub disconnect_hook: Option<Arc<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>>,
    pub subscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    pub unsubscribe_hook: Option<Arc<Box<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>>>,
    pub publish_hook: Option<Arc<Box<dyn Fn(&str, &Bytes, ProtocolVersion) -> Result<(), String> + Send + Sync>>>,
}

impl BrokerState {
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
            topic_subscribers: HashMap::new(),
            retained_messages: HashMap::new(),
            shared_subscribers: HashMap::new(),
            message_tx,
            connect_hook,
            disconnect_hook,
            subscribe_hook,
            unsubscribe_hook,
            publish_hook,
        }
    }

    pub fn add_client(&mut self, client_id: String, stream: Arc<Mutex<TcpStream>>) {
        let client = Client {
            _id: client_id.clone(),
            stream,
            subscriptions: HashSet::new(),
        };
        self.clients.insert(client_id.clone(), client);
    }

    pub fn remove_client(&mut self, client_id: &str) {
        if self.clients.remove(client_id).is_some() {
            self.topic_subscribers.retain(|_, subs| {
                subs.remove(client_id);
                !subs.is_empty()
            });
            self.shared_subscribers.retain(|_, subs| {
                subs.retain(|id| id != client_id);
                !subs.is_empty()
            });
            if let Some(hook) = self.disconnect_hook.as_ref() {
                if let Err(e) = hook(client_id) {
                    error!("Disconnect hook failed for client {}: {}", client_id, e);
                }
            }
        }
    }

    pub async fn add_subscription(&mut self, client_id: &str, topic: String) {
        if self.clients.contains_key(client_id) {
            let is_shared = topic.starts_with("$share/");
            if is_shared {
                let parts: Vec<&str> = topic.splitn(3, '/').collect();
                if parts.len() >= 3 {
                    let group = parts[1].to_string();
                    let actual_topic = parts[2..].join("/");
                    self.shared_subscribers
                        .entry(group)
                        .or_insert_with(Vec::new)
                        .push(client_id.to_string());
                    self.topic_subscribers
                        .entry(actual_topic.clone())
                        .or_insert_with(HashSet::new)
                        .insert(client_id.to_string());
                }
            } else {
                self.topic_subscribers
                    .entry(topic.clone())
                    .or_insert_with(HashSet::new)
                    .insert(client_id.to_string());
            }
            if let Some(client) = self.clients.get_mut(client_id) {
                client.subscriptions.insert(topic.clone());
            }
            if let Some((payload, version)) = self.retained_messages.get(&topic) {
                let _ = self.send_message(topic.clone(), payload.clone(), *version).await;
            }
        }
    }

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
    }

    pub fn get_subscribers(&self, topic: &str) -> Vec<String> {
        let mut subscribers = Vec::new();
        if let Some(subs) = self.topic_subscribers.get(topic) {
            subscribers.extend(subs.iter().cloned());
        }
        for (group, clients) in &self.shared_subscribers {
            if self.topic_subscribers.get(topic).is_some() {
                if let Some(client_id) = clients.first() {
                    subscribers.push(client_id.clone());
                }
            }
        }
        subscribers
    }

    pub fn store_retained_message(&mut self, topic: String, payload: Bytes, version: ProtocolVersion) {
        if payload.is_empty() {
            self.retained_messages.remove(&topic);
        } else {
            self.retained_messages.insert(topic, (payload, version));
        }
    }

    pub async fn send_message(&mut self, topic: String, payload: Bytes, version: ProtocolVersion) -> Result<(), MqttError> {
        self.message_tx.send((topic, payload, version)).await
            .map_err(|_| MqttError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Message channel closed")))?;
        Ok(())
    }

    pub async fn publish(&mut self, topic: &str, payload: Bytes, version: ProtocolVersion) -> Result<(), MqttError> {
        let subscribers = self.get_subscribers(topic);
        let mut disconnected_clients = Vec::new();

        for sub_id in subscribers.iter() {
            if let Some(hook) = self.publish_hook.as_ref() {
                if let Err(e) = hook(topic, &payload, version) {
                    error!("Publish hook failed for topic {} to client {}: {}", topic, sub_id, e);
                }
            }
            if let Some(client) = self.clients.get(sub_id) {
                let stream = client.stream.clone();
                let topic = topic.to_string();
                let payload = payload.clone();
                let sub_id = sub_id.clone();
                let result = tokio::spawn(async move {
                    let mut stream = stream.lock().await;
                    let (_, mut write_stream) = tokio::io::split(&mut *stream);
                    let publish_packet = [
                        vec![0x30, (2 + topic.len() + payload.len()) as u8, 0x00, topic.len() as u8],
                        topic.as_bytes().to_vec(),
                        payload.to_vec(),
                    ]
                    .concat();
                    write_stream.write_all(&publish_packet).await?;
                    write_stream.flush().await
                })
                .await;

                if let Err(e) = result {
                    error!("Task failed for client {}: {}", sub_id, e);
                    disconnected_clients.push(sub_id.clone());
                }
            }
        }

        for client_id in disconnected_clients {
            self.remove_client(&client_id);
        }

        Ok(())
    }
}