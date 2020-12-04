
# emqx-web-hook

EMQ X WebHook plugin.

Please see: [EMQ X - WebHook](https://docs.emqx.io/broker/latest/en/advanced/webhook.html)

## emqx_web_hook.conf

```properties
## The web services URL for Hook request
##
## Value: String
web.hook.api.url = http://127.0.0.1:8080

## Encode message payload field
##
## Value: base64 | base62
## web.hook.encode_payload = base64

##--------------------------------------------------------------------
## Hook Rules

## These configuration items represent a list of events should be forwarded
##
## Format:
##   web.hook.rule.<HookName>.<No> = <Spec>
web.hook.rule.client.connect.1       = {"action": "on_client_connect"}
web.hook.rule.client.connack.1       = {"action": "on_client_connack"}
web.hook.rule.client.connected.1     = {"action": "on_client_connected"}
web.hook.rule.client.disconnected.1  = {"action": "on_client_disconnected"}
web.hook.rule.client.subscribe.1     = {"action": "on_client_subscribe"}
web.hook.rule.client.unsubscribe.1   = {"action": "on_client_unsubscribe"}
web.hook.rule.session.subscribed.1   = {"action": "on_session_subscribed"}
web.hook.rule.session.unsubscribed.1 = {"action": "on_session_unsubscribed"}
web.hook.rule.session.terminated.1   = {"action": "on_session_terminated"}
web.hook.rule.message.publish.1      = {"action": "on_message_publish"}
web.hook.rule.message.delivered.1    = {"action": "on_message_delivered"}
web.hook.rule.message.acked.1        = {"action": "on_message_acked"}
```

## API

The HTTP request parameter format:

* client.connected
```json
{
    "action":"client_connected",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "keepalive": 60,
    "ipaddress": "127.0.0.1",
    "proto_ver": 4,
    "connected_at": 1556176748,
    "conn_ack":0
}
```

* client.disconnected
```json
{
    "action":"client_disconnected",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "reason":"normal"
}
```

* client.subscribe
```json
{
    "action":"client_subscribe",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "topic":"world",
    "opts":{
        "qos":0
    }
}
```

* client.unsubscribe
```json
{
    "action":"client_unsubscribe",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "topic":"world"
}
```

* session.created
```json
{
    "action":"session_created",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117"
}
```

* session.subscribed
```json
{
    "action":"session_subscribed",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "topic":"world",
    "opts":{
        "qos":0
    }
}
```

* session.unsubscribed
```json
{
    "action":"session_unsubscribed",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "topic":"world"
}
```

* session.terminated
```json
{
    "action":"session_terminated",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "reason":"normal"
}
```

* message.publish
```json
{
    "action":"message_publish",
    "from_client_id":"C_1492410235117",
    "from_username":"C_1492410235117",
    "topic":"world",
    "qos":0,
    "retain":true,
    "payload":"Hello world!",
    "ts":1492412774
}
```

* message.delivered
```json
{
    "action":"message_delivered",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "from_client_id":"C_1492410235117",
    "from_username":"C_1492410235117",
    "topic":"world",
    "qos":0,
    "retain":true,
    "payload":"Hello world!",
    "ts":1492412826
}
```

* message.acked
```json
{
    "action":"message_acked",
    "clientid":"C_1492410235117",
    "username":"C_1492410235117",
    "from_client_id":"C_1492410235117",
    "from_username":"C_1492410235117",
    "topic":"world",
    "qos":1,
    "retain":true,
    "payload":"Hello world!",
    "ts":1492412914
}
```

## License

Apache License Version 2.0

## Author

* [Sakib Sami](https://github.com/s4kibs4mi)

## Contributors

* [Deng](https://github.com/turtleDeng)
* [vishr](https://github.com/vishr)
* [emqplus](https://github.com/emqplus)
* [huangdan](https://github.com/huangdan)
