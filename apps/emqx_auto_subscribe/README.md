# Auto Subscribe

This application can help clients automatically subscribe to topics compiled from user definitions when they connect, and the clients no longer need to send the MQTT `SUBSCRIBE ` request.

# How To Use

Add the following configuration items to the `emqx.conf` file

```yaml
auto_subscribe {
    topics = [
        {
            topic = "c/${clientid}"
        },
        {
            topic = "client/${clientid}/username/${username}/host/${host}/port/${port}"
            qos   = 1
            rh    = 0
            rap   = 0
            nl    = 0
        }
    ]
}
```

This example defines two templates, all of which will be compiled into the final topic by replacing placeholders like `${clientid}` `${port}` with the actual values when the client connects.

# Configuration

## Configuration Definition

| Field          | Definition                    | Value Range                                                 | Default |
| -------------- | ----------------------------- | ----------------------------------------------------------- | ------- |
| auto_subscribe | Auto subscribe configuration  | topics                                                      | topics  |
| topics         | Subscription Options          | Subscription configurations list. See `Subscription Option` | []      |

## Subscription Option

| Field | Definition                                                                                              | Value Range                                                     | Default          |
|-------|---------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|------------------|
| topic | Required. Topic template.                                                                               | String, placeholders supported                                  | No default value |
| qos   | Optional. Subscription QoS                                                                              | 0 or 1 or 2. Refer to the MQTT QoS definition                   | 0                |
| rh    | Optional. MQTT version 5.0. Whether to send retain message when a subscription is created.              | 0: Not send the retain message </br>1: Send  the retain message | 0                |
| rap   | Optional. MQTT version 5.0. When forwarding messages, Whether to send with retain flag                  | 0: Set retain 0</br>1: Keep retain flag                         | 0                |
| nl    | Optional. MQTT version 5.0. Whether the message can be forwarded to the client when published by itself | 0: Forwarded to self</br>1: Not forwarded to self               | 0                |

## Subscription Placeholders

| Placeholder | Definition                             |
| ----------- | -------------------------------------- |
| ${clientid} | Client ID                              |
| ${username} | Client Username                        |
| ${ip}       | Client TCP connection local IP address |
| ${port}     | Client TCP connection local Port       |
