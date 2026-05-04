The MQTT parser now runs in strict mode by default. To restore the previous lenient behavior, set `mqtt.strict_mode = false` (globally or per-zone).

In strict mode, the broker validates incoming MQTT packets against the protocol specification and disconnects clients that send malformed packets. The validations enforced only in strict mode are:

- **Fixed-header flags.** Reserved DUP/QoS/RETAIN bits must be zero for non-PUBLISH packets, and PUBREL/SUBSCRIBE/UNSUBSCRIBE must use QoS=1 (`bad_frame_header`).
- **CONNECT reserved bit** must be zero (`reserved_connect_flag`).
- **CONNECT Will flag consistency**: Will Flag=0 requires Will QoS=0 and Will Retain=0; Will Flag=1 requires Will QoS in {0,1,2} (`invalid_will_qos`, `invalid_will_retain`).
- **CONNECT Password/Username flags (MQTT 3.1.1 only).** If Username Flag=0, Password Flag must also be 0, per `[MQTT-3.1.2-22]` (`invalid_password_flag`). MQTT 5.0 lifts this constraint and is unaffected.
- **UTF-8 strings** (proto name, client ID, topic, username, password, will topic, MQTT 5 string properties) must be valid UTF-8 and must not contain control characters U+0000–U+001F or U+007F–U+009F (`utf8_string_invalid`).
- **Packet identifiers** must be non-zero where required (PUBLISH QoS>0, PUBACK/REC/REL/COMP, SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK) (`bad_packet_id`).

When a client violates one of these checks, the broker logs an `info`-level entry with `msg=frame_parse_error` and a structured `reason` (e.g. `cause=invalid_password_flag`, `proto_ver`, `received_prefix`, …) for troubleshooting. For MQTT 5.0 connections the broker also responds with CONNACK/DISCONNECT carrying reason code `0x81 Malformed Packet` before closing; for MQTT 3.1/3.1.1 the connection is silently closed (no CONNACK reason code is defined for malformed packets in those versions).
