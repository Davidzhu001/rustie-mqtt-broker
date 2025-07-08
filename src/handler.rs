use crate::error::MqttError;
use crate::protocol::ProtocolVersion;
use crate::state::BrokerState;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
use futures_util::sink::SinkExt;
use bytes::Bytes;

/// Trait for streams that can write MQTT responses (TCP or WebSocket).
#[async_trait::async_trait]
pub trait WriteStream {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), MqttError>;
    async fn flush(&mut self) -> Result<(), MqttError>;
    async fn close(&mut self, close_frame: Option<CloseFrame<'_>>) -> Result<(), MqttError>;
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

    async fn close(&mut self, _close_frame: Option<CloseFrame<'_>>) -> Result<(), MqttError> {
        AsyncWriteExt::shutdown(self).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl WriteStream for tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), MqttError> {
        self.send(Message::Binary(data.to_vec())).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), MqttError> {
        Ok(())
    }

    async fn close(&mut self, close_frame: Option<CloseFrame<'_>>) -> Result<(), MqttError> {
        self.close(close_frame).await?;
        Ok(())
    }
}

/// Handles MQTT CONNECT packet for both TCP and WebSocket streams.
///
/// # Arguments
/// * `read_stream` - The stream to read the CONNECT packet from.
/// * `write_stream` - The stream to write the CONNACK response to.
/// * `data` - The CONNECT packet data.
/// * `auth_callback` - Callback to authenticate username and password.
///
/// # Returns
/// A tuple containing the client ID, clean session flag, and protocol version, or an error.
pub async fn handle_connect<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
) -> Result<(String, bool, ProtocolVersion), MqttError> {
    let mut offset = 0;
    let protocol_name = crate::packet::parse_mqtt_string(data, &mut offset)?;
    let protocol_version = match (protocol_name.as_str(), data.get(offset).copied()) {
        ("MQIsdp", Some(3)) => ProtocolVersion::V3_0,
        ("MQTT", Some(3)) => ProtocolVersion::V3_1,
        ("MQTT", Some(4)) => ProtocolVersion::V3_1_1,
        ("MQTT", Some(5)) => ProtocolVersion::V5_0,
        _ => {
            let connack = vec![0x20, 0x02, 0x00, 0x01];
            write_stream.write_all(&connack).await?;
            write_stream.flush().await?;
            return Err(MqttError::Protocol(format!(
                "Unsupported protocol '{}' or version",
                protocol_name
            )));
        }
    };
    offset += 1;

    let connect_flags = data[offset];
    let clean_session = (connect_flags & 0x02) != 0;
    let username_flag = (connect_flags & 0x80) != 0;
    let password_flag = (connect_flags & 0x40) != 0;
    offset += 1;

    let _keep_alive = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    if matches!(protocol_version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        crate::packet::parse_properties(data, &mut offset)?;
    }

    let client_id = crate::packet::parse_mqtt_string(data, &mut offset)?;
    if client_id.is_empty() {
        let connack = vec![0x20, 0x02, 0x00, 0x02];
        write_stream.write_all(&connack).await?;
        write_stream.flush().await?;
        return Err(MqttError::Protocol("Empty client ID".to_string()));
    }

    let mut authenticated = true;
    if username_flag && password_flag {
        let username = crate::packet::parse_mqtt_string(data, &mut offset)?;
        let password = crate::packet::parse_mqtt_string(data, &mut offset)?;
        authenticated = auth_callback(&username, &password);
    } else if username_flag || password_flag {
        authenticated = false;
    }

    if matches!(protocol_version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) && offset < data.len() {
        crate::packet::parse_properties(data, &mut offset)?;
    }

    let return_code = if authenticated { 0x00 } else { 0x04 };
    let connack = if matches!(protocol_version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        vec![0x20, 0x03, 0x00, return_code, 0x00]
    } else {
        vec![0x20, 0x02, 0x00, return_code]
    };
    write_stream.write_all(&connack).await?;
    write_stream.flush().await?;

    if !authenticated {
        return Err(MqttError::AuthFailed);
    }

    Ok((client_id, clean_session, protocol_version))
}

/// Handles MQTT SUBSCRIBE packet for both TCP and WebSocket streams.
///
/// # Arguments
/// * `read_stream` - The stream to read the SUBSCRIBE packet from.
/// * `write_stream` - The stream to write the SUBACK response to.
/// * `data` - The SUBSCRIBE packet data.
/// * `client_id` - The client ID.
/// * `state` - The shared broker state.
/// * `version` - The MQTT protocol version.
///
/// # Returns
/// Ok on success, or an error on failure.
pub async fn handle_subscribe<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    client_id: &str,
    state: Arc<Mutex<BrokerState>>,
    version: ProtocolVersion,
) -> Result<(), MqttError> {
    let mut offset = 0;
    let packet_id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    if matches!(version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        crate::packet::parse_properties(data, &mut offset)?;
    }

    let mut return_codes = Vec::new();
    while offset < data.len() {
        let topic = crate::packet::parse_mqtt_string(data, &mut offset)?;
        if offset >= data.len() {
            return Err(MqttError::Protocol("Missing QoS in SUBSCRIBE packet".to_string()));
        }
        let qos = data[offset];
        offset += 1;

        if qos > 1 {
            return_codes.push(0x80);
        } else {
            let mut state_guard = state.lock().await;
            state_guard.add_subscription(client_id, topic.clone());
            if let Some(hook) = state_guard.subscribe_hook.as_ref() {
                if let Err(e) = hook(client_id, &topic) {
                    tracing::warn!("Subscription hook failed for client {} on topic {}: {}", client_id, topic, e);
                }
            }
            return_codes.push(qos);
        }
    }

    let mut suback = if matches!(version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        vec![0x90, (3 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8, 0x00]
    } else {
        vec![0x90, (2 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8]
    };
    suback.extend(return_codes);
    write_stream.write_all(&suback).await?;
    write_stream.flush().await?;

    Ok(())
}

/// Handles MQTT UNSUBSCRIBE packet for both TCP and WebSocket streams.
///
/// # Arguments
/// * `read_stream` - The stream to read the UNSUBSCRIBE packet from.
/// * `write_stream` - The stream to write the UNSUBACK response to.
/// * `data` - The UNSUBSCRIBE packet data.
/// * `client_id` - The client ID.
/// * `state` - The shared broker state.
/// * `version` - The MQTT protocol version.
///
/// # Returns
/// Ok on success, or an error on failure.
pub async fn handle_unsubscribe<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    client_id: &str,
    state: Arc<Mutex<BrokerState>>,
    version: ProtocolVersion,
) -> Result<(), MqttError> {
    let mut offset = 0;
    let packet_id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    if matches!(version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        crate::packet::parse_properties(data, &mut offset)?;
    }

    let mut return_codes = Vec::new();
    while offset < data.len() {
        let topic = crate::packet::parse_mqtt_string(data, &mut offset)?;
        let mut state_guard = state.lock().await;
        state_guard.remove_subscription(client_id, &topic);
        if let Some(hook) = state_guard.unsubscribe_hook.as_ref() {
            if let Err(e) = hook(client_id, &topic) {
                tracing::warn!("Unsubscribe hook failed for client {} on topic {}: {}", client_id, topic, e);
            }
        }
        return_codes.push(0x00); // Success
    }

    let mut unsuback = if matches!(version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        vec![0xB0, (3 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8, 0x00]
    } else {
        vec![0xB0, (2 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8]
    };
    unsuback.extend(return_codes);
    write_stream.write_all(&unsuback).await?;
    write_stream.flush().await?;

    Ok(())
}

/// Handles MQTT PUBLISH packet.
///
/// # Arguments
/// * `data` - The PUBLISH packet data.
/// * `state` - The shared broker state.
/// * `version` - The MQTT protocol version.
///
/// # Returns
/// Ok on success, or an error on failure.
pub async fn handle_publish(
    data: &Bytes,
    state: Arc<Mutex<BrokerState>>,
    version: ProtocolVersion,
) -> Result<(), MqttError> {
    let mut offset = 0;
    let topic = crate::packet::parse_mqtt_string(data, &mut offset)?;
    let qos = (data[0] >> 1) & 0x03;

    let _packet_id = if qos > 0 {
        if offset + 2 > data.len() {
            return Err(MqttError::Protocol("Missing packet ID in PUBLISH packet".to_string()));
        }
        let id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
        offset += 2;
        Some(id)
    } else {
        None
    };

    if matches!(version, ProtocolVersion::V5_0 | ProtocolVersion::V5_1) {
        crate::packet::parse_properties(data, &mut offset)?;
    }

    let payload = Bytes::from(data[offset..].to_vec());

    let mut state_guard = state.lock().await;
    if let Some(hook) = state_guard.publish_hook.as_ref() {
        if let Err(e) = hook(&topic, &payload, version) {
            tracing::warn!("Publish hook failed for topic {}: {}", topic, e);
        }
    }
    state_guard.send_message(topic, payload, version).await?;

    Ok(())
}