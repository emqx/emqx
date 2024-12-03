## Breaking Changes
The following are no longer supported:
- The self-describing template has been removed
- One rule can only carry one `payload`, arrays of payloads are no longer supported
- The `data type` is now a plain value, not a template value
- The REST API driver only supports IoTDB 1.3.x and later

## Enhancements
- Thrift driver has add support for "batch" mode

**NOTE**:
To avoid overlapping timestamps in a batch, it is better to use the MQTT message timestamp (`${timestamp}`) or carry a certain time in the payload (for example `${payload.time}`) in batch mode.
