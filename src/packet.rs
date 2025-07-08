use crate::error::MqttError;
use crate::protocol::ProtocolVersion;
use tokio::io::{AsyncRead, AsyncReadExt};
use bytes::Bytes;

pub async fn read_fixed_header<R: AsyncRead + Unpin>(read_stream: &mut R) -> Result<(u8, u32, ProtocolVersion), MqttError> {
    let first_byte = read_stream.read_u8().await?;
    let packet_type = first_byte >> 4;
    let mut remaining_length = 0u32;
    let mut multiplier = 1u32;
    let mut byte;

    for i in 0..4 {
        byte = read_stream.read_u8().await?;
        remaining_length += ((byte & 0x7F) as u32) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if i == 3 && byte & 0x80 != 0 {
            return Err(MqttError::Protocol("Invalid remaining length".to_string()));
        }
    }

    Ok((packet_type, remaining_length, ProtocolVersion::V3_1_1))
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

pub fn parse_properties(data: &[u8], offset: &mut usize) -> Result<(), MqttError> {
    let mut prop_len = 0u32;
    let mut multiplier = 1u32;
    for i in 0..4 {
        if *offset >= data.len() {
            return Err(MqttError::Protocol("Invalid property length".to_string()));
        }
        let byte = data[*offset];
        prop_len += ((byte & 0x7F) as u32) * multiplier;
        *offset += 1;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if i == 3 && byte & 0x80 != 0 {
            return Err(MqttError::Protocol("Invalid property length".to_string()));
        }
    }
    *offset += prop_len as usize;
    Ok(())
}