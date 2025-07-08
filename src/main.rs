use rustie_mqtt_broker::{Broker, BrokerConfig};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt().init();
    let config = BrokerConfig::new(Some("127.0.0.1:1883"), Some("127.0.0.1:1884"))
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
        .with_max_connections(100000)
        .with_max_packet_size(1024 * 1024); // 1MB
    let mut broker = Broker::new(config);
    broker.start().await;
    Ok(())
}