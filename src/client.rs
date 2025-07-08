use crate::handler::{handle_connect, handle_publish, handle_subscribe, handle_unsubscribe, WriteStream};
use crate::protocol::{ControlPacketType, ProtocolVersion};
use crate::state::BrokerState;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{accept_async};
use tokio_tungstenite::tungstenite::{Message};
use tracing::{error, info, warn};
use futures_util::stream::StreamExt;
use futures_util::sink::SinkExt;
use bytes::Bytes;

pub async fn handle_client(
    stream: TcpStream,
    state: Arc<Mutex<BrokerState>>,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
) {
    let stream = Arc::new(Mutex::new(stream));
    let mut current_client_id: Option<String> = None;
    let mut protocol_version = ProtocolVersion::V3_1_1;

    loop {
        let mut stream_guard = stream.lock().await;
        let (mut read_stream, mut write_stream) = tokio::io::split(&mut *stream_guard);
        match timeout(Duration::from_secs(60), crate::packet::read_fixed_header(&mut read_stream)).await {
            Ok(Ok((packet_type, remaining_length, _version))) => {
                match crate::packet::read_bytes(&mut read_stream, remaining_length as usize).await {
                    Ok(data) => match packet_type {
                        t if t == ControlPacketType::Connect as u8 => {
                            match handle_connect(&mut read_stream, &mut write_stream, &data, auth_callback.clone()).await {
                                Ok((client_id, clean_session, version)) => {
                                    {
                                        let mut state = state.lock().await;
                                        if clean_session {
                                            state.remove_client(&client_id);
                                        }
                                        state.add_client(client_id.clone(), stream.clone());
                                        if let Some(hook) = state.connect_hook.as_ref() {
                                            if let Err(e) = hook(&client_id) {
                                                warn!("Connect hook failed for client {}: {}", client_id, e);
                                            }
                                        }
                                    }
                                    current_client_id = Some(client_id.clone());
                                    protocol_version = version;
                                    info!("Client {} connected with protocol {:?}", client_id, version);
                                }
                                Err(e) => {
                                    error!("Error handling CONNECT: {}", e);
                                    if let Some(client_id) = current_client_id.take() {
                                        state.lock().await.remove_client(&client_id);
                                    }
                                    write_stream.close(None).await.unwrap_or(());
                                    break;
                                }
                            }
                        }
                        t if t == ControlPacketType::Subscribe as u8 => {
                            if let Some(client_id) = current_client_id.as_ref() {
                                if let Err(e) = handle_subscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone(), protocol_version).await {
                                    error!("Error handling SUBSCRIBE: {}", e);
                                    if let Some(client_id) = current_client_id.take() {
                                        state.lock().await.remove_client(&client_id);
                                    }
                                    write_stream.close(None).await.unwrap_or(());
                                    break;
                                }
                            } else {
                                error!("SUBSCRIBE received before CONNECT");
                                write_stream.close(None).await.unwrap_or(());
                                break;
                            }
                        }
                        t if t == ControlPacketType::Unsubscribe as u8 => {
                            if let Some(client_id) = current_client_id.as_ref() {
                                if let Err(e) = handle_unsubscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone(), protocol_version).await {
                                    error!("Error handling UNSUBSCRIBE: {}", e);
                                    if let Some(client_id) = current_client_id.take() {
                                        state.lock().await.remove_client(&client_id);
                                    }
                                    write_stream.close(None).await.unwrap_or(());
                                    break;
                                }
                            } else {
                                error!("UNSUBSCRIBE received before CONNECT");
                                write_stream.close(None).await.unwrap_or(());
                                break;
                            }
                        }
                        t if t == ControlPacketType::Publish as u8 => {
                            if let Err(e) = handle_publish(&data, state.clone(), protocol_version).await {
                                error!("Error handling PUBLISH: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                write_stream.close(None).await.unwrap_or(());
                                break;
                            }
                        }
                        t if t == ControlPacketType::Pingreq as u8 => {
                            if let Err(e) = WriteStream::write_all(&mut write_stream, &[0xD0, 0x00]).await {
                                error!("Error sending PINGRESP: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                write_stream.close(None).await.unwrap_or(());
                                break;
                            }
                            if let Err(e) = WriteStream::flush(&mut write_stream).await {
                                error!("Error flushing PINGRESP: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                write_stream.close(None).await.unwrap_or(());
                                break;
                            }
                        }
                        t if t == ControlPacketType::Disconnect as u8 => {
                            if let Some(client_id) = current_client_id.take() {
                                state.lock().await.remove_client(&client_id);
                                info!("Client {} disconnected", client_id);
                            }
                            write_stream.close(None).await.unwrap_or(());
                            break;
                        }
                        _ => {
                            warn!("Unsupported packet type: {}", packet_type);
                        }
                    }
                    Err(e) => {
                        error!("Error reading packet data: {}", e);
                        if let Some(client_id) = current_client_id.take() {
                            state.lock().await.remove_client(&client_id);
                        }
                        write_stream.close(None).await.unwrap_or(());
                        break;
                    }
                }
            }
            Ok(Err(e)) => {
                error!("Error reading packet: {}", e);
                if let Some(client_id) = current_client_id.take() {
                    state.lock().await.remove_client(&client_id);
                }
                write_stream.close(None).await.unwrap_or(());
                break;
            }
            Err(_) => {
                error!("Client timed out");
                if let Some(client_id) = current_client_id.take() {
                    state.lock().await.remove_client(&client_id);
                    info!("Client {} disconnected due to timeout", client_id);
                }
                write_stream.close(None).await.unwrap_or(());
                break;
            }
        }
    }
}

pub async fn handle_ws_client(
    stream: TcpStream,
    state: Arc<Mutex<BrokerState>>,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
) {
    match accept_async(stream).await {
        Ok(ws_stream) => {
            let ws_stream = Arc::new(Mutex::new(ws_stream));
            let mut current_client_id: Option<String> = None;
            let mut protocol_version = ProtocolVersion::V3_1_1;

            loop {
                let mut ws_stream_guard = ws_stream.lock().await;
                match ws_stream_guard.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        let mut cursor = std::io::Cursor::new(Bytes::from(data));
                        match crate::packet::read_fixed_header(&mut cursor).await {
                            Ok((packet_type, _remaining_length, _version)) => {
                                let read_bytes = Bytes::from(cursor.get_ref().slice(2..));
                                match packet_type {
                                    t if t == ControlPacketType::Connect as u8 => {
                                        match handle_connect(&mut cursor, &mut *ws_stream_guard, &read_bytes, auth_callback.clone()).await {
                                            Ok((client_id, clean_session, version)) => {
                                                {
                                                    let mut state = state.lock().await;
                                                    if clean_session {
                                                        state.remove_client(&client_id);
                                                    }
                                                    state.add_ws_client(client_id.clone(), ws_stream.clone());
                                                    if let Some(hook) = state.connect_hook.as_ref() {
                                                        if let Err(e) = hook(&client_id) {
                                                            warn!("Connect hook failed for client {}: {}", client_id, e);
                                                        }
                                                    }
                                                }
                                                current_client_id = Some(client_id.clone());
                                                protocol_version = version;
                                                info!("WebSocket Client {} connected with protocol {:?}", client_id, version);
                                            }
                                            Err(e) => {
                                                error!("Error handling WebSocket CONNECT: {}", e);
                                                if let Some(client_id) = current_client_id.take() {
                                                    state.lock().await.remove_client(&client_id);
                                                }
                                                ws_stream_guard.close(None).await.unwrap_or(());
                                                break;
                                            }
                                        }
                                    }
                                    t if t == ControlPacketType::Subscribe as u8 => {
                                        if let Some(client_id) = current_client_id.as_ref() {
                                            if let Err(e) = handle_subscribe(&mut cursor, &mut *ws_stream_guard, &read_bytes, client_id, state.clone(), protocol_version).await {
                                                error!("Error handling WebSocket SUBSCRIBE: {}", e);
                                                if let Some(client_id) = current_client_id.take() {
                                                    state.lock().await.remove_client(&client_id);
                                                }
                                                ws_stream_guard.close(None).await.unwrap_or(());
                                                break;
                                            }
                                        } else {
                                            error!("WebSocket SUBSCRIBE received before CONNECT");
                                            ws_stream_guard.close(None).await.unwrap_or(());
                                            break;
                                        }
                                    }
                                    t if t == ControlPacketType::Unsubscribe as u8 => {
                                        if let Some(client_id) = current_client_id.as_ref() {
                                            if let Err(e) = handle_unsubscribe(&mut cursor, &mut *ws_stream_guard, &read_bytes, client_id, state.clone(), protocol_version).await {
                                                error!("Error handling WebSocket UNSUBSCRIBE: {}", e);
                                                if let Some(client_id) = current_client_id.take() {
                                                    state.lock().await.remove_client(&client_id);
                                                }
                                                ws_stream_guard.close(None).await.unwrap_or(());
                                                break;
                                            }
                                        } else {
                                            error!("WebSocket UNSUBSCRIBE received before CONNECT");
                                            ws_stream_guard.close(None).await.unwrap_or(());
                                            break;
                                        }
                                    }
                                    t if t == ControlPacketType::Publish as u8 => {
                                        if let Err(e) = handle_publish(&read_bytes, state.clone(), protocol_version).await {
                                            error!("Error handling WebSocket PUBLISH: {}", e);
                                            if let Some(client_id) = current_client_id.take() {
                                                state.lock().await.remove_client(&client_id);
                                            }
                                            ws_stream_guard.close(None).await.unwrap_or(());
                                            break;
                                        }
                                    }
                                    t if t == ControlPacketType::Pingreq as u8 => {
                                        if let Err(e) = ws_stream_guard.send(Message::Binary(Bytes::from(vec![0xD0, 0x00]).to_vec())).await {
                                            error!("Error sending WebSocket PINGRESP: {}", e);
                                            if let Some(client_id) = current_client_id.take() {
                                                state.lock().await.remove_client(&client_id);
                                            }
                                            ws_stream_guard.close(None).await.unwrap_or(());
                                            break;
                                        }
                                    }
                                    t if t == ControlPacketType::Disconnect as u8 => {
                                        if let Some(client_id) = current_client_id.take() {
                                            state.lock().await.remove_client(&client_id);
                                            info!("WebSocket Client {} disconnected", client_id);
                                        }
                                        ws_stream_guard.close(None).await.unwrap_or(());
                                        break;
                                    }
                                    _ => {
                                        warn!("Unsupported WebSocket packet type: {}", packet_type);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error reading WebSocket packet: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                ws_stream_guard.close(None).await.unwrap_or(());
                                break;
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        warn!("Received non-binary WebSocket message");
                    }
                    Some(Err(e)) => {
                        error!("WebSocket error: {}", e);
                        if let Some(client_id) = current_client_id.take() {
                            state.lock().await.remove_client(&client_id);
                        }
                        ws_stream_guard.close(None).await.unwrap_or(());
                        break;
                    }
                    None => {
                        error!("WebSocket connection closed");
                        if let Some(client_id) = current_client_id.take() {
                            state.lock().await.remove_client(&client_id);
                            info!("WebSocket Client {} disconnected", client_id);
                        }
                        ws_stream_guard.close(None).await.unwrap_or(());
                        break;
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to accept WebSocket connection: {}", e);
        }
    }
}