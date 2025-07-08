use crate::error::MqttError;
use crate::handler::WriteStream;
use crate::state::BrokerState;
use crate::packet::{parse_mqtt_string, parse_properties, Property};
use crate::ProtocolVersion;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use bytes::Bytes;
use tracing::{info, error, warn};

pub async fn handle_connect<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
) -> Result<(String, bool, u16), MqttError> {
    let mut offset = 0;
    let protocol_name = parse_mqtt_string(data, &mut offset)?;
    if protocol_name != "MQTT" || data.get(offset).copied() != Some(5) {
        let connack = vec![0x20, 0x03, 0x00, 0x01, 0x00]; // Unsupported Protocol Version
        write_stream.write_all(&connack).await?;
        write_stream.flush().await?;
        return Err(MqttError::Protocol(format!("Unsupported protocol '{}' or version", protocol_name)));
    }
    offset += 1;

    let connect_flags = data[offset];
    let clean_session = (connect_flags & 0x02) != 0;
    let username_flag = (connect_flags & 0x80) != 0;
    let password_flag = (connect_flags & 0x40) != 0;
    offset += 1;

    let keep_alive = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    parse_properties(data, &mut offset)?;

    let client_id = parse_mqtt_string(data, &mut offset)?;
    if client_id.is_empty() {
        let connack = vec![0x20, 0x03, 0x00, 0x02, 0x00]; // Client Identifier Not Valid
        write_stream.write_all(&connack).await?;
        write_stream.flush().await?;
        return Err(MqttError::Protocol("Empty client ID".to_string()));
    }

    let mut authenticated = true;
    if username_flag && password_flag {
        let username = parse_mqtt_string(data, &mut offset)?;
        let password = parse_mqtt_string(data, &mut offset)?;
        authenticated = auth_callback(&username, &password);
    } else if username_flag || password_flag {
        authenticated = false;
    }

    if offset < data.len() {
        parse_properties(data, &mut offset)?;
    }

    let return_code = if authenticated { 0x00 } else { 0x04 }; // Bad User Name or Password
    let connack = vec![0x20, 0x03, 0x00, return_code, 0x00];
    write_stream.write_all(&connack).await?;
    write_stream.flush().await?;

    if !authenticated {
        return Err(MqttError::AuthFailed);
    }

    Ok((client_id, clean_session, keep_alive))
}

pub async fn handle_subscribe<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    client_id: &str,
    state: Arc<Mutex<BrokerState>>,
) -> Result<(), MqttError> {
    let mut offset = 0;
    let packet_id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    parse_properties(data, &mut offset).map_err(|e| {
        let suback = vec![0x90, 0x04, (packet_id >> 8) as u8, packet_id as u8, 0x00, 0x87]; // Malformed Packet
        write_stream.write_all(&suback);
        write_stream.flush();
        e
    })?;

    let mut return_codes = Vec::new();
    while offset < data.len() {
        let topic = parse_mqtt_string(data, &mut offset)?;
        if offset >= data.len() {
            let suback = vec![0x90, 0x04, (packet_id >> 8) as u8, packet_id as u8, 0x00, 0x87]; // Malformed Packet
            write_stream.write_all(&suback);
            write_stream.flush();
            return Err(MqttError::Protocol("Missing QoS in SUBSCRIBE packet".to_string()));
        }
        let qos = data[offset];
        offset += 1;

        if qos > 2 {
            return_codes.push(0x80); // QoS Not Supported
        } else {
            let mut state_guard = state.lock().await;
            state_guard.add_subscription(client_id, topic.clone()).await;
            if let Some(hook) = state_guard.subscribe_hook.as_ref() {
                if let Err(e) = hook(client_id, &topic) {
                    error!("Subscription hook failed for client {} on topic {}: {}", client_id, topic, e);
                }
            }
            return_codes.push(qos);
        }
    }

    let mut suback = vec![0x90, (3 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8, 0x00];
    suback.extend(return_codes);
    write_stream.write_all(&suback).await?;
    write_stream.flush().await?;

    Ok(())
}

pub async fn handle_unsubscribe<R: AsyncRead + Unpin, W: WriteStream>(
    _read_stream: &mut R,
    write_stream: &mut W,
    data: &Bytes,
    client_id: &str,
    state: Arc<Mutex<BrokerState>>,
) -> Result<(), MqttError> {
    let mut offset = 0;
    let packet_id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
    offset += 2;

    parse_properties(data, &mut offset).map_err(|e| {
        let unsuback = vec![0xB0, 0x04, (packet_id >> 8) as u8, packet_id as u8, 0x00, 0x87]; // Malformed Packet
        write_stream.write_all(&unsuback);
        write_stream.flush();
        e
    })?;

    let mut return_codes = Vec::new();
    while offset < data.len() {
        let topic = parse_mqtt_string(data, &mut offset)?;
        let mut state_guard = state.lock().await;
        state_guard.remove_subscription(client_id, &topic);
        if let Some(hook) = state_guard.unsubscribe_hook.as_ref() {
            if let Err(e) = hook(client_id, &topic) {
                error!("Unsubscribe hook failed for client {} on topic {}: {}", client_id, topic, e);
            }
        }
        return_codes.push(0x00); // Success
    }

    let mut unsuback = vec![0xB0, (3 + return_codes.len()) as u8, (packet_id >> 8) as u8, packet_id as u8, 0x00];
    unsuback.extend(return_codes);
    write_stream.write_all(&unsuback).await?;
    write_stream.flush().await?;

    Ok(())
}

pub async fn handle_publish<W: WriteStream>(
    data: &Bytes,
    state: Arc<Mutex<BrokerState>>,
    write_stream: &mut W,
) -> Result<(), MqttError> {
    let start_time = Instant::now();
    let mut offset = 0;

    let topic = parse_mqtt_string(data, &mut offset).map_err(|e| {
        if (data[0] >> 1) & 0x03 > 0 {
            let id = ((data[2] as u16) << 8) | (data[3] as u16);
            let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, 0x87]; // Malformed Packet
            write_stream.write_all(&puback);
            write_stream.flush();
        }
        e
    })?;
    if topic.is_empty() || topic.contains(&['+', '#'][..]) {
        if (data[0] >> 1) & 0x03 > 0 {
            let id = ((data[2] as u16) << 8) | (data[3] as u16);
            let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, 0x82]; // Topic Name Invalid
            write_stream.write_all(&puback);
            write_stream.flush();
        }
        return Ok(()); // Continue session
    }

    let qos = (data[0] >> 1) & 0x03;
    let retain = (data[0] & 0x01) != 0;
    let packet_id = if qos > 0 {
        if offset + 2 > data.len() {
            let puback = vec![0x40, 0x03, 0x00, 0x00, 0x87]; // Malformed Packet
            write_stream.write_all(&puback).await?;
            write_stream.flush().await?;
            return Ok(()); // Continue session
        }
        let id = ((data[offset] as u16) << 8) | (data[offset + 1] as u16);
        offset += 2;
        Some(id)
    } else {
        None
    };

    if offset < data.len() {
        let _ = parse_properties(data, &mut offset).map_err(|e| {
            let reason_code = match e {
                MqttError::Protocol(ref msg) if msg.contains("Unsupported property ID") => 0x81, // Protocol Error
                _ => 0x87, // Malformed Packet
            };
            if qos == 1 {
                if let Some(id) = packet_id {
                    let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, reason_code];
                    write_stream.write_all(&puback);
                    let _ = write_stream.flush();
                }
            } else if qos == 2 {
                if let Some(id) = packet_id {
                    let pubrec = vec![0x50, 0x03, (id >> 8) as u8, id as u8, reason_code];
                    write_stream.write_all(&pubrec);
                    write_stream.flush();
                }
            }
            Ok(()) as Result<(), MqttError>
        });
    }

    if offset > data.len() {
        if qos > 0 {
            let id = packet_id.unwrap_or(0);
            let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, 0x87]; // Malformed Packet
            write_stream.write_all(&puback).await?;
            write_stream.flush().await?;
        }
        return Ok(()); // Continue session
    }

    let payload = Bytes::from(data[offset..].to_vec());

    if retain {
        let mut state_guard = state.lock().await;
        state_guard.store_retained_message(topic.clone(), payload.clone(), ProtocolVersion::V5_0);
    }

    {
        let state_guard = state.lock().await;
        if let Some(hook) = state_guard.publish_hook.as_ref() {
            if let Err(e) = hook(&topic, &payload, ProtocolVersion::V5_0) {
                error!("Publish hook failed for topic {}: {}", topic, e);
            }
        }
    }

    let message_tx = {
        let state_guard = state.lock().await;
        state_guard.message_tx.clone()
    };

    if let Err(_) = timeout(
        Duration::from_secs(5),
        message_tx.send((topic, payload, ProtocolVersion::V5_0))
    ).await {
        warn!("Failed to send message to channel: channel full or closed");
        if qos == 1 {
            if let Some(id) = packet_id {
                let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, 0x8B]; // Packet Too Large
                write_stream.write_all(&puback).await?;
                write_stream.flush().await?;
            }
        } else if qos == 2 {
            if let Some(id) = packet_id {
                let pubrec = vec![0x50, 0x03, (id >> 8) as u8, id as u8, 0x8B]; // Packet Too Large
                write_stream.write_all(&pubrec).await?;
                write_stream.flush().await?;
            }
        }
        return Ok(()); // Continue session
    }

    if qos == 1 {
        if let Some(id) = packet_id {
            let puback = vec![0x40, 0x03, (id >> 8) as u8, id as u8, 0x00]; // Success
            write_stream.write_all(&puback).await?;
            write_stream.flush().await?;
        }
    } else if qos == 2 {
        if let Some(id) = packet_id {
            let pubrec = vec![0x50, 0x03, (id >> 8) as u8, id as u8, 0x00]; // Success
            write_stream.write_all(&pubrec).await?;
            write_stream.flush().await?;
            // Note: Full QoS 2 handling requires PUBREL and PUBCOMP
        }
    }

    info!("Processed PUBLISH packet in {:?}", start_time.elapsed());
    Ok(())
}