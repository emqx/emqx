Change `mqtt.retry_interval` config default to `infinity`.

Previously, the default value for `retry_interval` was 30 seconds.

The new default has been changed to 'infinity'. With this update, EMQX will not automatically retry message deliveries by default.

Aligning with MQTT specification standards, in-session message delivery retries are not typically compliant.
We recognize that some users depend on this feature, so the option to configure retries remains available for backward compatibility.
