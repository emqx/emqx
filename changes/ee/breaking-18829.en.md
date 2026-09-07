MQTT listeners now bound what a client can send before it is connected. Two new settings limit the CONNECT packet:

- `mqtt.max_connect_packet_size` limits the size of a CONNECT packet. Default: `64KB`.
- `mqtt.max_connect_user_properties` limits how many `User-Property` pairs a CONNECT may carry, counted separately for the CONNECT properties and the will properties. Default: `10`.

A CONNECT above either limit is refused and the connection is closed. Both limits apply to CONNECT only; other packet types are unchanged.

Previously a CONNECT could be as large as `mqtt.max_packet_size` (1MB by default) and carry any number of user properties, so a client that had not connected yet could tie up that much memory on the broker.

Clients that send unusually large credentials or last-will payloads, or more than 10 user properties in CONNECT, need the matching setting raised.
