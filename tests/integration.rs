use rustie_mqtt_broker::{Broker, BrokerConfig, MqttError};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use paho_mqtt::{AsyncClient, CreateOptionsBuilder, Message};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use futures_util::{SinkExt, StreamExt};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

// Shared state to capture hook calls
#[derive(Clone)]
struct TestHooks {
    calls: Arc<Mutex<Vec<String>>>,
}

impl TestHooks {
    fn new() -> Self {
        TestHooks {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

async fn start_broker(hooks: TestHooks) -> (Broker, tokio::sync::oneshot::Sender<()>) {
    let config = BrokerConfig::new("127.0.0.1:1883")
        .with_ws_address("127.0.0.1:9001")
        .with_auth(|username, password| username == "admin" && password == "password")
        .with_connect_hook({
            let hooks = hooks.clone();
            move |client_id| {
                hooks.calls.lock().unwrap().push(format!("connect:{}", client_id));
                Ok(())
            }
        })
        .with_disconnect_hook({
            let hooks = hooks.clone();
            move |client_id| {
                hooks.calls.lock().unwrap().push(format!("disconnect:{}", client_id));
                Ok(())
            }
        })
        .with_subscribe_hook({
            let hooks = hooks.clone();
            move |client_id, topic| {
                hooks.calls.lock().unwrap().push(format!("subscribe:{}:{}", client_id, topic));
                Ok(())
            }
        })
        .with_unsubscribe_hook({
            let hooks = hooks.clone();
            move |client_id, topic| {
                hooks.calls.lock().unwrap().push(format!("unsubscribe:{}:{}", client_id, topic));
                Ok(())
            }
        })
        .with_publish_hook({
            let hooks = hooks.clone();
            move |topic, payload, _version| {
                hooks.calls.lock().unwrap().push(format!("publish:{}:{:?}", topic, payload));
                Ok(())
            }
        })
        .with_max_connections(10);

    let mut broker = Broker::new(config);
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = broker.start().await;
        let _ = rx.await;
    });
    (broker, tx)
}

#[tokio::test]
async fn test_tcp_connect_disconnect() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    let client = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("test-client")
            .finalize(),
    )
    .unwrap();

    // Connect with valid credentials
    client
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("admin")
                .password("password")
                .finalize(),
        )
        .await
        .unwrap();

    // Disconnect
    client.disconnect(None).await.unwrap();

    // Allow time for hooks to execute
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify hooks
    let calls = hooks.get_calls();
    assert_eq!(
        calls,
        vec![
            "connect:test-client".to_string(),
            "disconnect:test-client".to_string()
        ]
    );

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_tcp_connect_invalid_auth() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    let client = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("test-client")
            .finalize(),
    )
    .unwrap();

    // Connect with invalid credentials
    let result = client
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("wrong")
                .password("wrong")
                .finalize(),
        )
        .await;

    assert!(result.is_err());
    assert_eq!(hooks.get_calls(), Vec::<String>::new()); // No hooks should be called

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_tcp_subscribe_publish() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    let client1 = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("client1")
            .finalize(),
    )
    .unwrap();

    let client2 = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("client2")
            .finalize(),
    )
    .unwrap();

    // Connect clients
    client1
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("admin")
                .password("password")
                .finalize(),
        )
        .await
        .unwrap();
    client2
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("admin")
                .password("password")
                .finalize(),
        )
        .await
        .unwrap();

    // Subscribe client1 to a topic
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    client1.set_message_callback(move |_, msg| {
        if let Some(msg) = msg {
            received_clone.lock().unwrap().push(msg.payload().to_vec());
        }
    });
    client1.subscribe("test/topic", 0).await.unwrap();

    // Publish from client2
    client2
        .publish(Message::new("test/topic", "Hello, MQTT!", 0))
        .await
        .unwrap();

    // Wait for message delivery
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify received message
    let received_msgs = received.lock().unwrap();
    assert_eq!(received_msgs.len(), 1);
    assert_eq!(received_msgs[0], b"Hello, MQTT!");

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:client1".to_string()));
    assert!(calls.contains(&"connect:client2".to_string()));
    assert!(calls.contains(&"subscribe:client1:test/topic".to_string()));
    assert!(calls.contains(&"publish:test/topic:[72, 101, 108, 108, 111, 44, 32, 77, 81, 84, 84, 33]".to_string()));

    // Disconnect clients
    client1.disconnect(None).await.unwrap();
    client2.disconnect(None).await.unwrap();

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"disconnect:client1".to_string()));
    assert!(calls.contains(&"disconnect:client2".to_string()));

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_tcp_unsubscribe() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    let client = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("test-client")
            .finalize(),
    )
    .unwrap();

    // Connect
    client
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("admin")
                .password("password")
                .finalize(),
        )
        .await
        .unwrap();

    // Subscribe
    client.subscribe("test/topic", 0).await.unwrap();

    // Unsubscribe
    client.unsubscribe("test/topic").await.unwrap();

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:test-client".to_string()));
    assert!(calls.contains(&"subscribe:test-client:test/topic".to_string()));
    assert!(calls.contains(&"unsubscribe:test-client:test/topic".to_string()));

    // Disconnect
    client.disconnect(None).await.unwrap();

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hook
    assert!(hooks.get_calls().contains(&"disconnect:test-client".to_string()));

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_ws_connect_disconnect() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    // Connect WebSocket client
    let (mut ws_stream, _) = connect_async("ws://127.0.0.1:9001")
        .await
        .expect("Failed to connect WebSocket");

    // Send CONNECT packet
    let connect_packet = vec![
        0x10, // Packet type: CONNECT
        0x12, // Remaining length
        0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // Protocol name: MQTT
        0x04, // Protocol version: 3.1.1
        0xC2, // Connect flags: username, password, clean session
        0x00, 0x3C, // Keep alive: 60s
        0x00, 0x04, 0x74, 0x65, 0x73, 0x74, // Client ID: test
        0x00, 0x05, 0x61, 0x64, 0x6D, 0x69, 0x6E, // Username: admin
        0x00, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64, // Password: password
    ];
    ws_stream.send(WsMessage::Binary(connect_packet)).await.unwrap();

    // Receive CONNACK
    let msg = timeout(Duration::from_millis(100), ws_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(msg, WsMessage::Binary(ref data) if data.starts_with(&[0x20, 0x02, 0x00, 0x00])));

    // Send DISCONNECT packet
    let disconnect_packet = vec![0xE0, 0x00];
    ws_stream.send(WsMessage::Binary(disconnect_packet)).await.unwrap();

    // Close WebSocket
    ws_stream.close(None).await.unwrap();

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify hooks
    let calls = hooks.get_calls();
    assert_eq!(
        calls,
        vec![
            "connect:test".to_string(),
            "disconnect:test".to_string()
        ]
    );

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}

#[tokio::test]
async fn test_timeout_disconnect() {
    let hooks = TestHooks::new();
    let (_broker, shutdown_tx) = start_broker(hooks.clone()).await;

    let client = AsyncClient::new(
        CreateOptionsBuilder::new()
            .server_uri("tcp://127.0.0.1:1883")
            .client_id("test-client")
            .finalize(),
    )
    .unwrap();

    // Connect
    client
        .connect(
            paho_mqtt::ConnectOptionsBuilder::new()
                .user_name("admin")
                .password("password")
                .finalize(),
        )
        .await
        .unwrap();

    // Wait for timeout (60s in handle_client)
    tokio::time::sleep(Duration::from_secs(61)).await;

    // Verify disconnect hook
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:test-client".to_string()));
    assert!(calls.contains(&"disconnect:test-client".to_string()));

    // Shutdown broker
    shutdown_tx.send(()).unwrap();
}