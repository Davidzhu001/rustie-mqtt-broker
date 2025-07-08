#[allow(dead_code)]
#[repr(u8)]
pub enum ControlPacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProtocolVersion {
    V3_0,    // MQIsdp, version 3
    V3_1,    // MQTT, version 3
    V3_1_1,  // MQTT, version 4
    V5_0,    // MQTT, version 5
    V5_1,    // MQTT, version 5 (same as 5.0 for this implementation)
}