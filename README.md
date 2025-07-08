# Rustie MQTT Broker 

## ⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️ Not Ready to use yet!!!

Rustie MQTT Broker is a lightweight, asynchronous MQTT broker implemented in Rust, supporting MQTT versions 3.0, 3.1, 3.1.1, and 5.0 over both TCP and WebSocket connections. It provides customizable hooks for connection, disconnection, subscription, unsubscription, and message publishing events.

## Features
- Supports MQTT protocols: MQIsdp (3.0), MQTT 3.1, 3.1.1, and 5.0.
- Handles TCP and WebSocket (MQTT over WebSocket) connections.
- Customizable authentication and event hooks.
- Configurable maximum concurrent connections.
- Graceful shutdown and client disconnection handling.

## Installation

### Prerequisites
- Rust (stable, version 1.65 or higher)
- Cargo (Rust's package manager)

### Dependencies
Add the following to your `Cargo.toml`:

```toml
[dependencies]
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
tokio-tungstenite = "0.23"
futures-util = "0.3"
bytes = "1"
tracing = "0.1"
thiserror = "1"

[dev-dependencies]
paho-mqtt = "0.12"
tokio = { version = "1", features = ["full", "test-util"] }
```

### Building
Clone the repository and build the project:

```bash
git clone <repository-url>
cd rustie-mqtt-broker
cargo build --release
```

## Usage

### Running the Broker
The broker can be started with a configuration specifying the TCP and WebSocket addresses, authentication, hooks, and maximum connections.

#### Example: Basic Configuration
Create a `main.rs` file with the following:

```rust
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
```

Run the broker:

```bash
cargo run
```

This starts the broker listening on `127.0.0.1:1883` for TCP connections and `127.0.0.1:9001` for WebSocket connections, with authentication requiring username "admin" and password "password".

### Connecting Clients

#### TCP Client
Use an MQTT client library like `paho-mqtt` (Python, Rust, etc.) to connect to `tcp://127.0.0.1:1883`. Example in Python:

```python
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("test/topic")

def on_message(client, userdata, msg):
    print(f"Received: {msg.payload.decode()} on {msg.topic}")

client = mqtt.Client(client_id="test-client")
client.username_pw_set("admin", "password")
client.on_connect = on_connect
client.on_message = on_message
client.connect("127.0.0.1", 1883)
client.loop_start()

client.publish("test/topic", "Hello, MQTT!")
client.loop_stop()
```

#### WebSocket Client
Connect to `ws://127.0.0.1:9001` using an MQTT-over-WebSocket client. Example using `paho-mqtt` in Python:

```python
import paho.mqtt.client as mqtt

client = mqtt.Client(client_id="test-client", transport="websockets")
client.username_pw_set("admin", "password")
client.connect("127.0.0.1", 9001)
client.subscribe("test/topic")
client.publish("test/topic", "Hello via WebSocket!")
```

### Hooks
The broker supports hooks for the following events:
- **Connect**: Triggered when a client connects successfully.
- **Disconnect**: Triggered when a client disconnects (via DISCONNECT packet, timeout, or error).
- **Subscribe**: Triggered when a client subscribes to a topic.
- **Unsubscribe**: Triggered when a client unsubscribes from a topic.
- **Publish**: Triggered when a message is published to a topic (for each subscriber).

Hooks are configured in `BrokerConfig` and can be used for logging, validation, or integration with external systems. See the example above for hook configuration.

### Testing
Run the integration tests to verify broker functionality:

```bash
cargo test
```

The test suite (`tests/integration.rs`) includes:
- TCP and WebSocket connect/disconnect tests.
- Subscribe, unsubscribe, and publish tests.
- Timeout and authentication failure tests.
- Hook execution verification.

## Error Handling
The broker uses a custom `MqttError` enum for errors:
- `Io`: I/O-related errors.
- `Protocol`: MQTT protocol violations (e.g., invalid packet).
- `AuthFailed`: Authentication failures.
- `WebSocket`: WebSocket-specific errors.
- `Join`: Task join errors.
- `Hook`: Hook execution failures.

Errors are logged using the `tracing` crate for debugging.

## Configuration Options
- `address`: TCP listen address (e.g., "127.0.0.1:1883").
- `ws_address`: WebSocket listen address (e.g., "127.0.0.1:9001").
- `auth_callback`: Custom authentication function.
- `max_connections`: Maximum concurrent connections.
- `connect_hook`, `disconnect_hook`, `subscribe_hook`, `unsubscribe_hook`, `publish_hook`: Custom event handlers.

## Contributing
Contributions are welcome! Please submit pull requests or open issues on the repository.

## License
This project is licensed under the MIT License.
