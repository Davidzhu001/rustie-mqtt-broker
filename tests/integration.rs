use rustie_mqtt_broker::{Broker, BrokerConfig};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use futures_util::{SinkExt, StreamExt};

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

async fn start_broker(hooks: TestHooks) -> tokio::sync::oneshot::Sender<()> {
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
    tx
}

#[tokio::test]
async fn test_tcp_connect_disconnect() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options = MqttOptions::new("test-client", "tcp://127.0.0.1:1883", 1883);
    mqtt_options.set_credentials("admin", "password");
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    // Connect and wait for CONNACK
    let mut connected = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected = true;
            break;
        }
    }
    assert!(connected, "Failed to receive CONNACK");

    // Disconnect
    client.disconnect().await.expect("Failed to disconnect");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify hooks
    let calls = hooks.get_calls();
    assert_eq!(
        calls,
        vec![
            "connect:test-client".to_string(),
            "disconnect:test-client".to_string()
        ],
        "Unexpected hook calls"
    );

    // Shutdown broker
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_tcp_connect_invalid_auth() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options = MqttOptions::new("test-client", "tcp://127.0.0.1:1883", 1883);
    mqtt_options.set_credentials("wrong", "wrong");
    let (_client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    // Connect (should fail)
    let result = timeout(Duration::from_millis(100), eventloop.poll()).await;
    assert!(result.is_err() || matches!(result, Ok(Err(_))), "Expected connection failure");

    // Verify no hooks called
    assert_eq!(hooks.get_calls(), Vec::<String>::new());

    // Shutdown broker
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_tcp_subscribe_publish() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options1 = MqttOptions::new("client1", "tcp://127.0.0.1:1883", 1883);
    mqtt_options1.set_credentials("admin", "password");
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let mut mqtt_options2 = MqttOptions::new("client2", "tcp://127.0.0.1:1883", 1883);
    mqtt_options2.set_credentials("admin", "password");
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    // Connect client1
    let mut connected1 = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop1.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected1 = true;
            break;
        }
    }
    assert!(connected1, "Client1 failed to connect");

    // Connect client2
    let mut connected2 = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop2.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected2 = true;
            break;
        }
    }
    assert!(connected2, "Client2 failed to connect");

    // Subscribe client1
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    client1.subscribe("test/topic", QoS::AtMostOnce).await.expect("Failed to subscribe");
    let client1_handle = tokio::spawn(async move {
        let mut subscribed = false;
        while let Ok(event) = eventloop1.poll().await {
            match event {
                Event::Incoming(Packet::SubAck(_)) => {
                    subscribed = true;
                }
                Event::Incoming(Packet::Publish(p)) => {
                    if subscribed {
                        received_clone.lock().unwrap().push(p.payload.to_vec());
                    }
                }
                _ => {}
            }
        }
    });

    // Wait for SUBACK
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish from client2
    client2
        .publish("test/topic", QoS::AtMostOnce, false, "Hello, MQTT!")
        .await
        .expect("Failed to publish");

    // Wait for message delivery
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify received message
    let received_msgs = received.lock().unwrap();
    assert_eq!(received_msgs.len(), 1, "Expected one message");
    assert_eq!(received_msgs[0], b"Hello, MQTT!", "Unexpected message payload");

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:client1".to_string()), "Missing connect:client1");
    assert!(calls.contains(&"connect:client2".to_string()), "Missing connect:client2");
    assert!(calls.contains(&"subscribe:client1:test/topic".to_string()), "Missing subscribe:client1:test/topic");
    assert!(calls.contains(&"publish:test/topic:[72, 101, 108, 108, 111, 44, 32, 77, 81, 84, 84, 33]".to_string()), "Missing publish");

    // Disconnect clients
    client1.disconnect().await.expect("Failed to disconnect client1");
    client2.disconnect().await.expect("Failed to disconnect client2");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"disconnect:client1".to_string()), "Missing disconnect:client1");
    assert!(calls.contains(&"disconnect:client2".to_string()), "Missing disconnect:client2");

    // Cleanup
    client1_handle.abort();
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_tcp_subscribe_publish_qos1() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options1 = MqttOptions::new("client1", "tcp://127.0.0.1:1883", 1883);
    mqtt_options1.set_credentials("admin", "password");
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let mut mqtt_options2 = MqttOptions::new("client2", "tcp://127.0.0.1:1883", 1883);
    mqtt_options2.set_credentials("admin", "password");
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    // Connect client1
    let mut connected1 = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop1.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected1 = true;
            break;
        }
    }
    assert!(connected1, "Client1 failed to connect");

    // Connect client2
    let mut connected2 = false;
    let client2_handle = tokio::spawn(async move {
        let mut connected = false;
        while let Ok(event) = eventloop2.poll().await {
            match event {
                Event::Incoming(Packet::ConnAck(_)) => {
                    connected = true;
                    break;
                }
                _ => {}
            }
        }
        (connected, eventloop2)
    });
    let (connected2, mut eventloop2) = client2_handle.await.unwrap();
    assert!(connected2, "Client2 failed to connect");

    // Subscribe client1
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    client1.subscribe("test/topic", QoS::AtLeastOnce).await.expect("Failed to subscribe");
    let client1_handle = tokio::spawn(async move {
        let mut subscribed = false;
        while let Ok(event) = eventloop1.poll().await {
            match event {
                Event::Incoming(Packet::SubAck(_)) => {
                    subscribed = true;
                }
                Event::Incoming(Packet::Publish(p)) => {
                    if subscribed {
                        received_clone.lock().unwrap().push(p.payload.to_vec());
                    }
                }
                _ => {}
            }
        }
    });

    // Wait for SUBACK
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish from client2 with QoS 1
    client2
        .publish("test/topic", QoS::AtLeastOnce, false, "Hello, MQTT!")
        .await
        .expect("Failed to publish");

    // Wait for PUBACK
    let mut puback_received = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop2.poll()).await.unwrap() {
        if let Event::Incoming(Packet::PubAck(_)) = event {
            puback_received = true;
            break;
        }
    }
    assert!(puback_received, "Failed to receive PUBACK");

    // Wait for message delivery
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify received message
    let received_msgs = received.lock().unwrap();
    assert_eq!(received_msgs.len(), 1, "Expected one message");
    assert_eq!(received_msgs[0], b"Hello, MQTT!", "Unexpected message payload");

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:client1".to_string()), "Missing connect:client1");
    assert!(calls.contains(&"connect:client2".to_string()), "Missing connect:client2");
    assert!(calls.contains(&"subscribe:client1:test/topic".to_string()), "Missing subscribe:client1:test/topic");
    assert!(calls.contains(&"publish:test/topic:[72, 101, 108, 108, 111, 44, 32, 77, 81, 84, 84, 33]".to_string()), "Missing publish");

    // Disconnect clients
    client1.disconnect().await.expect("Failed to disconnect client1");
    client2.disconnect().await.expect("Failed to disconnect client2");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"disconnect:client1".to_string()), "Missing disconnect:client1");
    assert!(calls.contains(&"disconnect:client2".to_string()), "Missing disconnect:client2");

    // Cleanup
    client1_handle.abort();
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_tcp_unsubscribe() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options = MqttOptions::new("test-client", "tcp://127.0.0.1:1883", 1883);
    mqtt_options.set_credentials("admin", "password");
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    // Connect
    let mut connected = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected = true;
            break;
        }
    }
    assert!(connected, "Failed to connect");

    // Subscribe
    client.subscribe("test/topic", QoS::AtMostOnce).await.expect("Failed to subscribe");
    let mut subscribed = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop.poll()).await.unwrap() {
        if let Event::Incoming(Packet::SubAck(_)) = event {
            subscribed = true;
            break;
        }
    }
    assert!(subscribed, "Failed to receive SUBACK");

    // Unsubscribe
    client.unsubscribe("test/topic").await.expect("Failed to unsubscribe");
    let mut unsubscribed = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop.poll()).await.unwrap() {
        if let Event::Incoming(Packet::UnsubAck(_)) = event {
            unsubscribed = true;
            break;
        }
    }
    assert!(unsubscribed, "Failed to receive UNSUBACK");

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:test-client".to_string()), "Missing connect:test-client");
    assert!(calls.contains(&"subscribe:test-client:test/topic".to_string()), "Missing subscribe:test-client:test/topic");
    assert!(calls.contains(&"unsubscribe:test-client:test/topic".to_string()), "Missing unsubscribe:test-client:test/topic");

    // Disconnect
    client.disconnect().await.expect("Failed to disconnect");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hook
    assert!(hooks.get_calls().contains(&"disconnect:test-client".to_string()), "Missing disconnect:test-client");

    // Shutdown broker
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_ws_connect_disconnect() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

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
    ws_stream.send(WsMessage::Binary(connect_packet)).await.expect("Failed to send CONNECT");

    // Receive CONNACK
    let msg = timeout(Duration::from_millis(100), ws_stream.next())
        .await
        .expect("Timed out waiting for CONNACK")
        .expect("No CONNACK received")
        .expect("Error receiving CONNACK");
    assert!(matches!(msg, WsMessage::Binary(ref data) if data.starts_with(&[0x20, 0x02, 0x00, 0x00])), "Invalid CONNACK");

    // Send DISCONNECT packet
    let disconnect_packet = vec![0xE0, 0x00];
    ws_stream.send(WsMessage::Binary(disconnect_packet)).await.expect("Failed to send DISCONNECT");

    // Close WebSocket
    ws_stream.close(None).await.expect("Failed to close WebSocket");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify hooks
    let calls = hooks.get_calls();
    assert_eq!(
        calls,
        vec![
            "connect:test".to_string(),
            "disconnect:test".to_string()
        ],
        "Unexpected hook calls"
    );

    // Shutdown broker
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_ws_subscribe_publish() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    // Connect WebSocket client
    let (mut ws_stream1, _) = connect_async("ws://127.0.0.1:9001")
        .await
        .expect("Failed to connect WebSocket client1");
    let (mut ws_stream2, _) = connect_async("ws://127.0.0.1:9001")
        .await
        .expect("Failed to connect WebSocket client2");

    // Send CONNECT packets
    let connect_packet = vec![
        0x10, 0x12, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xC2, 0x00, 0x3C,
        0x00, 0x07, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x31, // Client ID: client1
        0x00, 0x05, 0x61, 0x64, 0x6D, 0x69, 0x6E,
        0x00, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64,
    ];
    ws_stream1.send(WsMessage::Binary(connect_packet)).await.expect("Failed to send CONNECT client1");
    let connect_packet2 = vec![
        0x10, 0x12, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xC2, 0x00, 0x3C,
        0x00, 0x07, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x32, // Client ID: client2
        0x00, 0x05, 0x61, 0x64, 0x6D, 0x69, 0x6E,
        0x00, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64,
    ];
    ws_stream2.send(WsMessage::Binary(connect_packet2)).await.expect("Failed to send CONNECT client2");

    // Receive CONNACK for both clients
    let msg1 = timeout(Duration::from_millis(100), ws_stream1.next())
        .await
        .expect("Timed out waiting for CONNACK client1")
        .expect("No CONNACK received client1")
        .expect("Error receiving CONNACK client1");
    assert!(matches!(msg1, WsMessage::Binary(ref data) if data.starts_with(&[0x20, 0x02, 0x00, 0x00])), "Invalid CONNACK client1");

    let msg2 = timeout(Duration::from_millis(100), ws_stream2.next())
        .await
        .expect("Timed out waiting for CONNACK client2")
        .expect("No CONNACK received client2")
        .expect("Error receiving CONNACK client2");
    assert!(matches!(msg2, WsMessage::Binary(ref data) if data.starts_with(&[0x20, 0x02, 0x00, 0x00])), "Invalid CONNACK client2");

    // Subscribe client1
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let subscribe_packet = vec![
        0x82, // Packet type: SUBSCRIBE, QoS 1
        0x0C, // Remaining length
        0x00, 0x01, // Packet ID
        0x00, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2F, 0x74, 0x6F, 0x70, 0x69, 0x63, // Topic: test/topic
        0x00, // QoS: 0
    ];
    ws_stream1.send(WsMessage::Binary(subscribe_packet)).await.expect("Failed to send SUBSCRIBE");
    let msg = timeout(Duration::from_millis(100), ws_stream1.next())
        .await
        .expect("Timed out waiting for SUBACK")
        .expect("No SUBACK received")
        .expect("Error receiving SUBACK");
    assert!(matches!(msg, WsMessage::Binary(ref data) if data.starts_with(&[0x90])), "Invalid SUBACK");

    // Publish from client2
    let publish_packet = vec![
        0x30, // Packet type: PUBLISH, QoS 0
        0x14, // Remaining length
        0x00, 0x09, 0x74, 0x65, 0x73, 0x74, 0x2F, 0x74, 0x6F, 0x70, 0x69, 0x63, // Topic: test/topic
        0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x4D, 0x51, 0x54, 0x54, 0x21, // Payload: Hello, MQTT!
    ];
    ws_stream2.send(WsMessage::Binary(publish_packet)).await.expect("Failed to send PUBLISH");

    // Receive message on client1
    let msg = timeout(Duration::from_millis(200), ws_stream1.next())
        .await
        .expect("Timed out waiting for PUBLISH")
        .expect("No PUBLISH received")
        .expect("Error receiving PUBLISH");
    if let WsMessage::Binary(data) = msg {
        received_clone.lock().unwrap().push(data[14..].to_vec()); // Skip fixed header and topic
    }

    // Verify received message
    let received_msgs = received.lock().unwrap();
    assert_eq!(received_msgs.len(), 1, "Expected one message");
    assert_eq!(received_msgs[0], b"Hello, MQTT!", "Unexpected message payload");

    // Verify hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:client1".to_string()), "Missing connect:client1");
    assert!(calls.contains(&"connect:client2".to_string()), "Missing connect:client2");
    assert!(calls.contains(&"subscribe:client1:test/topic".to_string()), "Missing subscribe:client1:test/topic");
    assert!(calls.contains(&"publish:test/topic:[72, 101, 108, 108, 111, 44, 32, 77, 81, 84, 84, 33]".to_string()), "Missing publish");

    // Disconnect clients
    let disconnect_packet = vec![0xE0, 0x00];
    ws_stream1.send(WsMessage::Binary(disconnect_packet.clone())).await.expect("Failed to send DISCONNECT client1");
    ws_stream2.send(WsMessage::Binary(disconnect_packet)).await.expect("Failed to send DISCONNECT client2");

    // Close WebSocket
    ws_stream1.close(None).await.expect("Failed to close WebSocket client1");
    ws_stream2.close(None).await.expect("Failed to close WebSocket client2");

    // Wait for hooks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnect hooks
    let calls = hooks.get_calls();
    assert!(calls.contains(&"disconnect:client1".to_string()), "Missing disconnect:client1");
    assert!(calls.contains(&"disconnect:client2".to_string()), "Missing disconnect:client2");

    // Shutdown broker
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}

#[tokio::test]
async fn test_timeout_disconnect() {
    let hooks = TestHooks::new();
    let shutdown_tx = start_broker(hooks.clone()).await;

    let mut mqtt_options = MqttOptions::new("test-client", "tcp://127.0.0.1:1883", 1883);
    mqtt_options.set_credentials("admin", "password");
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    // Connect
    let mut connected = false;
    while let Ok(event) = timeout(Duration::from_millis(100), eventloop.poll()).await.unwrap() {
        if let Event::Incoming(Packet::ConnAck(_)) = event {
            connected = true;
            break;
        }
    }
    assert!(connected, "Failed to connect");

    // Wait for timeout (60s in handle_client)
    tokio::time::sleep(Duration::from_secs(61)).await;

    // Verify disconnect hook
    let calls = hooks.get_calls();
    assert!(calls.contains(&"connect:test-client".to_string()), "Missing connect:test-client");
    assert!(calls.contains(&"disconnect:test-client".to_string()), "Missing disconnect:test-client");

    // Cleanup
    let _ = client.disconnect().await;
    shutdown_tx.send(()).expect("Failed to shutdown broker");
}