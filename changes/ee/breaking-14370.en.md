## Breaking Changes
For IoTDB the following are no longer supported:
- The self-describing template has been removed.

  EMQX now only uses the configured data templates to process messages and no longer attempts to extract the template from the payload.

- One message can only carry one `payload`, arrays of payloads are no longer be supported

  Now, a MQTT message is processed as an atomic insert operation (a sigle or a batch insert) to IoTDB, and it is no longer possible to generate multiple IoTDB operations from a single MQTT message.

- The `data type` is now a plain value, not a template value
- The REST API driver only supports IoTDB 1.3.x and later

## Enhancements
- Thrift driver has add support for "batch" mode

**NOTE**:
To avoid overlapping timestamps in a batch, it is better to use the MQTT message timestamp (`${timestamp}`) or carry a certain time in the payload (for example `${payload.time}`) in batch mode.
