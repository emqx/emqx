# EMQX MQTT Broker Bridge

This application connects EMQX to virtually any MQTT broker adhering to either [MQTTv3][1] or [MQTTv5][2] standard. The connection is facilitated through the _MQTT bridge_ abstraction, allowing for the flow of data in both directions: from the remote broker to EMQX (ingress) and from EMQX to the remote broker (egress).

User can create a rule and easily ingest into a remote MQTT broker by leveraging [EMQX Rules][3].


# Documentation

- Refer to [Bridge Data into MQTT Broker][4] for how to use EMQX dashboard to set up ingress or egress bridge, or even both at the same time.

- Refer to [EMQX Rules][3] for the EMQX rules engine introduction.


# HTTP APIs

Several APIs are provided for bridge management, refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing guide](../../CONTRIBUTING.md).


# Refernces

[1]: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1
[2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
[3]: https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html
[4]: https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-mqtt.html
