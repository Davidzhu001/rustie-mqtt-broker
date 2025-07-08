use crate::handler::WriteStream;
use crate::packet::{read_packet};
use crate::protocol::{ControlPacketType, ProtocolVersion};
use crate::state::BrokerState;
use crate::v3;
use crate::v5;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use tracing::{error, info};
use bytes::Bytes;

pub async fn handle_client(
    stream: TcpStream,
    state: Arc<Mutex<BrokerState>>,
    auth_callback: Arc<Box<dyn Fn(&str, &str) -> bool + Send + Sync>>,
    protocol_version: ProtocolVersion,
    max_packet_size: usize,
) {
    let stream = Arc::new(Mutex::new(stream));
    let mut current_client_id: Option<String> = None;
    let mut timeout_duration = Duration::from_secs(10);

    loop {
        let mut stream_guard = stream.lock().await;
        let (mut read_stream, mut write_stream) = tokio::io::split(&mut *stream_guard);
        match timeout(timeout_duration, read_packet(&mut read_stream, max_packet_size)).await {
            Ok(Ok((packet_type, data))) => {
                match packet_type {
                    t if t == ControlPacketType::Connect as u8 => {
                        let result = if protocol_version == ProtocolVersion::V3_1_1 {
                            v3::handle_connect(&mut read_stream, &mut write_stream, &data, auth_callback.clone()).await
                        } else {
                            v5::handle_connect(&mut read_stream, &mut write_stream, &data, auth_callback.clone()).await
                        };
                        match result {
                            Ok((client_id, clean_session, keep_alive)) => {
                                {
                                    let mut state = state.lock().await;
                                    if clean_session {
                                        state.remove_client(&client_id);
                                    }
                                    state.add_client(client_id.clone(), stream.clone());
                                    if let Some(hook) = state.connect_hook.as_ref() {
                                        if let Err(e) = hook(&client_id) {
                                            error!("Connect hook failed for client {}: {}", client_id, e);
                                        }
                                    }
                                }
                                current_client_id = Some(client_id.clone());
                                timeout_duration = Duration::from_secs((keep_alive as u64 * 15) / 10);
                                info!("Client {} connected with protocol {:?}, keep_alive: {}s, timeout: {:?}", client_id, protocol_version, keep_alive, timeout_duration);
                            }
                            Err(e) => {
                                error!("Error handling CONNECT: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                let _ = write_stream.close(None).await;
                                break;
                            }
                        }
                    }
                    t if t == ControlPacketType::Subscribe as u8 => {
                        if let Some(client_id) = current_client_id.as_ref() {
                            let result = if protocol_version == ProtocolVersion::V3_1_1 {
                                v3::handle_subscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone()).await
                            } else {
                                v5::handle_subscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone()).await
                            };
                            if let Err(e) = result {
                                error!("Error handling SUBSCRIBE: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                let _ = write_stream.close(None).await;
                                break;
                            }
                        } else {
                            error!("SUBSCRIBE received before CONNECT");
                            let _ = write_stream.close(None).await;
                            break;
                        }
                    }
                    t if t == ControlPacketType::Unsubscribe as u8 => {
                        if let Some(client_id) = current_client_id.as_ref() {
                            let result = if protocol_version == ProtocolVersion::V3_1_1 {
                                v3::handle_unsubscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone()).await
                            } else {
                                v5::handle_unsubscribe(&mut read_stream, &mut write_stream, &data, client_id, state.clone()).await
                            };
                            if let Err(e) = result {
                                error!("Error handling UNSUBSCRIBE: {}", e);
                                if let Some(client_id) = current_client_id.take() {
                                    state.lock().await.remove_client(&client_id);
                                }
                                let _ = write_stream.close(None).await;
                                break;
                            }
                        } else {
                            error!("UNSUBSCRIBE received before CONNECT");
                            let _ = write_stream.close(None).await;
                            break;
                        }
                    }
                    t if t == ControlPacketType::Publish as u8 => {
                        let result = if protocol_version == ProtocolVersion::V3_1_1 {
                            v3::handle_publish(&data, state.clone(), &mut write_stream).await
                        } else {
                            v5::handle_publish(&data, state.clone(), &mut write_stream).await
                        };
                        if let Err(e) = result {
                            error!("Error handling PUBLISH: {}", e);
                            continue; // Continue session instead of disconnecting
                        }
                    }
                    t if t == ControlPacketType::Pingreq as u8 => {
                        if let Err(e) = WriteStream::write_all(&mut write_stream, &[0xD0, 0x00]).await {
                            error!("Error sending PINGRESP: {}", e);
                            if let Some(client_id) = current_client_id.take() {
                                state.lock().await.remove_client(&client_id);
                            }
                            let _ = write_stream.close(None).await;
                            break;
                        }
                        if let Err(e) = WriteStream::flush(&mut write_stream).await {
                            error!("Error flushing PINGRESP: {}", e);
                            if let Some(client_id) = current_client_id.take() {
                                state.lock().await.remove_client(&client_id);
                            }
                            let _ = write_stream.close(None).await;
                            break;
                        }
                    }
                    t if t == ControlPacketType::Disconnect as u8 => {
                        if let Some(client_id) = current_client_id.take() {
                            state.lock().await.remove_client(&client_id);
                            info!("Client {} disconnected", client_id);
                        }
                        let _ = write_stream.close(None).await;
                        break;
                    }
                    _ => {
                        error!("Unsupported packet type: {}", packet_type);
                        if let Some(client_id) = current_client_id.take() {
                            state.lock().await.remove_client(&client_id);
                        }
                        let _ = write_stream.close(None).await;
                        break;
                    }
                }
            }
            Ok(Err(e)) => {
                error!("Error reading packet: {}", e);
                if let Some(client_id) = current_client_id.take() {
                    state.lock().await.remove_client(&client_id);
                }
                let _ = write_stream.close(None).await;
                break;
            }
            Err(_) => {
                error!("Client timed out");
                if let Some(client_id) = current_client_id.take() {
                    state.lock().await.remove_client(&client_id);
                    info!("Client {} disconnected due to timeout", client_id);
                }
                let _ = write_stream.close(None).await;
                break;
            }
        }
    }
}