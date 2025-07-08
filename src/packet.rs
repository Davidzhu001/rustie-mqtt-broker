use crate::error::MqttError;
use tokio::io::{AsyncRead, AsyncReadExt};
use bytes::Bytes;
use tracing::{error, warn};

#[derive(Debug, Clone)]
pub enum Property {
    TopicAlias(u16),
    UserProperty(String, String),
    SessionExpiryInterval(u32),
    MaximumPacketSize(u32),
    SubscriptionIdentifier(u32),
}

pub async fn read_packet<R: AsyncRead + Unpin>(read_stream: &mut R, max_packet_size: usize) -> Result<(u8, Bytes), MqttError> {
    let first_byte = read_stream.read_u8().await?;
    let packet_type = first_byte >> 4;
    let mut remaining_length = 0u32;
    let mut multiplier = 1u32;

    for i in 0..4 {
        let byte = read_stream.read_u8().await?;
        remaining_length += ((byte & 0x7F) as u32) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if i == 3 && byte & 0x80 != 0 {
            return Err(MqttError::Protocol("Invalid remaining length".to_string()));
        }
    }

    if remaining_length as usize > max_packet_size {
        return Err(MqttError::PacketTooLarge(format!(
            "Packet size {} exceeds maximum allowed {}",
            remaining_length, max_packet_size
        )));
    }

    let mut buf = vec![0u8; remaining_length as usize];
    read_stream.read_exact(&mut buf).await?;
    Ok((packet_type, Bytes::from(buf)))
}

pub async fn read_bytes<R: AsyncRead + Unpin>(read_stream: &mut R, len: usize) -> Result<Bytes, MqttError> {
    let mut buf = vec![0u8; len];
    read_stream.read_exact(&mut buf).await?;
    Ok(Bytes::from(buf))
}

pub fn parse_mqtt_string(data: &[u8], offset: &mut usize) -> Result<String, MqttError> {
    if *offset + 2 > data.len() {
        return Err(MqttError::Protocol("Invalid string length".to_string()));
    }
    let len = ((data[*offset] as usize) << 8) | (data[*offset + 1] as usize);
    *offset += 2;
    if *offset + len > data.len() {
        return Err(MqttError::Protocol("Invalid string data".to_string()));
    }
    let s = String::from_utf8(data[*offset..*offset + len].to_vec())
        .map_err(|_| MqttError::Protocol("Invalid UTF-8 string".to_string()))?;
    *offset += len;
    Ok(s)
}

pub fn parse_properties(data: &[u8], offset: &mut usize) -> Result<Vec<Property>, MqttError> {
    if *offset >= data.len() {
        return Ok(Vec::new());
    }

    let start_offset = *offset;
    let mut prop_len = 0u32;
    let mut multiplier = 1u32;
    let mut prop_len_bytes = Vec::new();

    for i in 0..4 {
        if *offset >= data.len() {
            warn!(
                "Incomplete property length encoding at offset {}: data={:x?}",
                start_offset, &data[start_offset..]
            );
            return Err(MqttError::Protocol("Incomplete property length encoding".to_string()));
        }
        let byte = data[*offset];
        prop_len_bytes.push(byte);
        prop_len += ((byte & 0x7F) as u32) * multiplier;
        *offset += 1;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if i == 3 && byte & 0x80 != 0 {
            return Err(MqttError::Protocol("Invalid property length encoding".to_string()));
        }
    }

    if prop_len == 0 {
        return Ok(Vec::new());
    }

    let remaining_bytes = data.len() - *offset;
    if prop_len > remaining_bytes as u32 {
        error!(
            "Property length {} exceeds remaining packet length {} at offset {}: prop_len_bytes={:x?}, packet_data={:x?}",
            prop_len, remaining_bytes, start_offset, prop_len_bytes, &data[start_offset..]
        );
        return Err(MqttError::Protocol("Property length exceeds packet length".to_string()));
    }

    let prop_end = *offset + prop_len as usize;
    let mut properties = Vec::new();
    while *offset < prop_end {
        if *offset >= data.len() {
            error!(
                "Unexpected end of data while parsing properties: offset={}, prop_end={}, packet_data={:x?}",
                *offset, prop_end, &data[start_offset..]
            );
            return Err(MqttError::Protocol("Unexpected end of data in properties".to_string()));
        }
        let prop_id = data[*offset];
        *offset += 1;

        if prop_id == 0 {
            error!(
                "Invalid property ID 0 at offset {}: data={:x?}",
                *offset - 1, &data[*offset - 1..prop_end.min(data.len())]
            );
            return Err(MqttError::Protocol("Invalid property ID 0".to_string()));
        }

        match prop_id {
            0x01 => {
                if *offset + 2 > prop_end {
                    error!(
                        "Invalid topic alias property at offset {}: remaining_bytes={}",
                        *offset, prop_end - *offset
                    );
                    return Err(MqttError::Protocol("Invalid topic alias property".to_string()));
                }
                let alias = ((data[*offset] as u16) << 8) | (data[*offset + 1] as u16);
                *offset += 2;
                properties.push(Property::TopicAlias(alias));
            }
            0x11 => {
                if *offset + 4 > prop_end {
                    error!(
                        "Invalid session expiry interval property at offset {}: remaining_bytes={}",
                        *offset, prop_end - *offset
                    );
                    return Err(MqttError::Protocol("Invalid session expiry interval property".to_string()));
                }
                let interval = ((data[*offset] as u32) << 24)
                    | ((data[*offset + 1] as u32) << 16)
                    | ((data[*offset + 2] as u32) << 8)
                    | (data[*offset + 3] as u32);
                *offset += 4;
                properties.push(Property::SessionExpiryInterval(interval));
            }
            0x17 => {
                if *offset + 4 > prop_end {
                    error!(
                        "Invalid maximum packet size property at offset {}: remaining_bytes={}",
                        *offset, prop_end - *offset
                    );
                    return Err(MqttError::Protocol("Invalid maximum packet size property".to_string()));
                }
                let size = ((data[*offset] as u32) << 24)
                    | ((data[*offset + 1] as u32) << 16)
                    | ((data[*offset + 2] as u32) << 8)
                    | (data[*offset + 3] as u32);
                *offset += 4;
                properties.push(Property::MaximumPacketSize(size));
            }
            0x21 => {
                if *offset + 4 > prop_end {
                    error!(
                        "Invalid subscription identifier property at offset {}: remaining_bytes={}",
                        *offset, prop_end - *offset
                    );
                    return Err(MqttError::Protocol("Invalid subscription identifier property".to_string()));
                }
                let id = ((data[*offset] as u32) << 24)
                    | ((data[*offset + 1] as u32) << 16)
                    | ((data[*offset + 2] as u32) << 8)
                    | (data[*offset + 3] as u32);
                *offset += 4;
                properties.push(Property::SubscriptionIdentifier(id));
            }
            0x26 => {
                let key = parse_mqtt_string(data, offset)?;
                let value = parse_mqtt_string(data, offset)?;
                properties.push(Property::UserProperty(key, value));
            }
            _ => {
                error!(
                    "Unsupported property ID {} at offset {}: data={:x?}",
                    prop_id, *offset - 1, &data[*offset - 1..prop_end.min(data.len())]
                );
                return Err(MqttError::Protocol(format!("Unsupported property ID {}", prop_id)));
            }
        }
    }

    if *offset != prop_end {
        error!(
            "Property parsing did not consume expected length: offset={}, prop_end={}, packet_data={:x?}",
            *offset, prop_end, &data[start_offset..]
        );
        return Err(MqttError::Protocol("Property parsing did not consume expected length".to_string()));
    }

    Ok(properties)
}