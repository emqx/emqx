The `mqtt.clientid_override` expression no longer falls back to the client-supplied Client ID when it fails.

When `mqtt.clientid_override` is configured and the expression raises an error (for example, it references an attribute the client did not provide) or renders an empty string, EMQX now refuses the connection with CONNACK reason code 0x85 (Client Identifier not valid; return code 2 for MQTT 3.1 and 3.1.1 clients). Previously such clients stayed connected under their original Client ID, so the override silently did not apply to them.

Before upgrading, verify that every connecting client can render the configured expression to a non-empty string. Clients that could not render the expression connected with their original Client ID before the upgrade; after the upgrade they are refused until the expression or the client data is fixed.
