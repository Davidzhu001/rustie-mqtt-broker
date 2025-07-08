use rustie_mqtt_broker::{Broker, BrokerConfig};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt().init();
    let config = BrokerConfig::new("127.0.0.1:1883")
        .with_ws_address("127.0.0.1:9001")
        .with_auth(|username, password| {
            println!("Authenticating: username={}, password={}", username, password);
            username == "admin" && password == "password"
            
        })
        .with_connect_hook(|client_id| {
            println!("Client {} connected", client_id);
            Ok(())
        })
        .with_disconnect_hook(|client_id| {
            println!("Client {} disconnected", client_id);
            Ok(())
        })
        .with_subscribe_hook(|client_id, topic| {
            println!("Client {} subscribed to topic {}", client_id, topic);
            Ok(())
        })
        .with_unsubscribe_hook(|client_id, topic| {
            println!("Client {} unsubscribed from topic {}", client_id, topic);
            Ok(())
        })
        .with_publish_hook(|topic, payload, version| {
            println!("Published to topic {} with payload {:?} (version: {:?})", topic, payload, version);
            Ok(())
        })
        .with_max_connections(1000);
    let mut broker = Broker::new(config);
    let _ = broker.start().await;
    Ok(())
}