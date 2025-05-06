
# Retainer

The `emqx_retainer` application is responsible for storing retained MQTT messages.

The retained messages are messages associated with a topic that are stored on the broker and delivered to any new subscribers to that topic.

More information about retained messages can be found in the following resources
* [MQTT specification 3.3.1.3](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).
* Retained message concept in [EMQX documentation](https://www.emqx.io/docs/en/v5.0/messaging/mqtt-concepts.html#retained-message).
* Instructions for publishing retained messages in [EMQX documentation](https://www.emqx.io/docs/en/v5.0/messaging/mqtt-retained-message.html#publish-retained-message-with-mqttx-client).
* [The Beginner's Guide to MQTT Retained Messages](https://www.emqx.com/en/blog/mqtt5-features-retain-message).

## Usage

 The `emqx_retainer` application is enabled by default. To turn it off, add the following configuration to the `emqx.conf` file:

 ```
retainer {
    enable = false
}
```

For other options, see the [configuration](https://www.emqx.io/docs/en/v5.2/configuration/configuration-manual.html#retainer) documentation.

## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
