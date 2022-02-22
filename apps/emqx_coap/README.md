
# emqx-coap

emqx-coap is a CoAP Gateway for EMQX Broker. It translates CoAP messages into MQTT messages and make it possible to communiate between CoAP clients and MQTT clients.

### Client Usage Example
libcoap is an excellent coap library which has a simple client tool. It is recommended to use libcoap as a coap client.

To compile libcoap, do following steps:

```
git clone http://github.com/obgm/libcoap
cd libcoap
./autogen.sh
./configure --enable-documentation=no --enable-tests=no
make
```

### Publish example:
```
libcoap/examples/coap-client -m put -e 1234  "coap://127.0.0.1/mqtt/topic1?c=client1&u=tom&p=secret"
```
- topic name is "topic1", NOT "/topic1"
- client id is client1
- username is tom
- password is secret
- payload is a text string "1234"

A mqtt message with topic="topic1", payload="1234" has been published. Any mqtt client or coap client, who has subscribed this topic could receive this message immediately.

### Subscribe example:

```
libcoap/examples/coap-client -m get -s 10 "coap://127.0.0.1/mqtt/topic1?c=client1&u=tom&p=secret"
```
- topic name is "topic1", NOT "/topic1"
- client id is client1
- username is tom
- password is secret
- subscribe time is 10 seconds

And you will get following result if any mqtt client or coap client sent message with text "1234567" to "topic1":

```
v:1 t:CON c:GET i:31ae {} [ ]
1234567v:1 t:CON c:GET i:31af {} [ Observe:1, Uri-Path:mqtt, Uri-Path:topic1, Uri-Query:c=client1, Uri-Query:u=tom, Uri-Query:p=secret ]
```
The output message is not well formatted which hide "1234567" at the head of the 2nd line.

### Configure

#### Common

File: etc/emqx_coap.conf

```properties

## The UDP port that CoAP is listening on.
##
## Value: Port
coap.port = 5683

## Interval for keepalive, specified in seconds.
##
## Value: Duration
##  -s: seconds
##  -m: minutes
##  -h: hours
coap.keepalive = 120s

## Whether to enable statistics for CoAP clients.
##
## Value: on | off
coap.enable_stats = off

```

#### DTLS

emqx_coap enable one-way authentication by default.

If you want to disable it, comment these lines.

File: etc/emqx_coap.conf

```properties

## The DTLS port that CoAP is listening on.
##
## Value: Port
coap.dtls.port = 5684

## Private key file for DTLS
##
## Value: File
coap.dtls.keyfile = {{ platform_etc_dir }}/certs/key.pem

## Server certificate for DTLS.
##
## Value: File
coap.dtls.certfile = {{ platform_etc_dir }}/certs/cert.pem

```

##### Enable two-way autentication

For two-way autentication:

```properties

## A server only does x509-path validation in mode verify_peer,
## as it then sends a certificate request to the client (this
## message is not sent if the verify option is verify_none).
## You can then also want to specify option fail_if_no_peer_cert.
## More information at: http://erlang.org/doc/man/ssl.html
##
## Value: verify_peer | verify_none
## coap.dtls.verify = verify_peer

## PEM-encoded CA certificates for DTLS
##
## Value: File
## coap.dtls.cacertfile = {{ platform_etc_dir }}/certs/cacert.pem

## Used together with {verify, verify_peer} by an SSL server. If set to true,
## the server fails if the client does not have a certificate to send, that is,
## sends an empty certificate.
##
## Value: true | false
## coap.dtls.fail_if_no_peer_cert = false

```

### Load emqx-coap

```bash
./bin/emqx_ctl plugins load emqx_coap
```

CoAP Client Observe Operation (subscribe topic)
-----------------------------------------------
To subscribe any topic, issue following command:

```
  GET  coap://localhost/mqtt/{topicname}?c={clientid}&u={username}&p={password}    with OBSERVE=0
```

- "mqtt" in the path is mandatory.
- replace {topicname}, {clientid}, {username} and {password} with your true values.
- {topicname} and {clientid} is mandatory.
- if clientid is absent, a "bad_request" will be returned.
- {topicname} in URI should be percent-encoded to prevent special characters, such as + and #.
- {username} and {password} are optional.
- if {username} or {password} is incorrect, the error code `unauthorized` will be returned.
- topic is subscribed with qos1.
- if the subscription failed due to ACL deny, the error code `forbidden` will be returned.

CoAP Client Unobserve Operation (unsubscribe topic)
---------------------------------------------------
To cancel observation, issue following command:

```
  GET  coap://localhost/mqtt/{topicname}?c={clientid}&u={username}&p={password}    with OBSERVE=1
```

- "mqtt" in the path is mandatory.
- replace {topicname}, {clientid}, {username} and {password} with your true values.
- {topicname} and {clientid} is mandatory.
- if clientid is absent, a "bad_request" will be returned.
- {topicname} in URI should be percent-encoded to prevent special characters, such as + and #.
- {username} and {password} are optional.
- if {username} or {password} is incorrect, the error code `unauthorized` will be returned.

CoAP Client Notification Operation (subscribed Message)
-------------------------------------------------------
Server will issue an observe-notification as a subscribed message.

- Its payload is exactly the mqtt payload.
- payload data type is "application/octet-stream".

CoAP Client Publish Operation
-----------------------------
Issue a coap put command to publish messages. For example:

```
  PUT  coap://localhost/mqtt/{topicname}?c={clientid}&u={username}&p={password}
```

- "mqtt" in the path is mandatory.
- replace {topicname}, {clientid}, {username} and {password} with your true values.
- {topicname} and {clientid} is mandatory.
- if clientid is absent, a "bad_request" will be returned.
- {topicname} in URI should be percent-encoded to prevent special characters, such as + and #.
- {username} and {password} are optional.
- if {username} or {password} is incorrect, the error code `unauthorized` will be returned.
- payload could be any binary data.
- payload data type is "application/octet-stream".
- publish message will be sent with qos0.
- if the publishing failed due to ACL deny, the error code `forbidden` will be returned.

CoAP Client Keep Alive
----------------------
Device should issue a get command periodically, serve as a ping to keep mqtt session online.

```
  GET  coap://localhost/mqtt/{any_topicname}?c={clientid}&u={username}&p={password}
```

- "mqtt" in the path is mandatory.
- replace {any_topicname}, {clientid}, {username} and {password} with your true values.
- {any_topicname} is optional, and should be percent-encoded to prevent special characters.
- {clientid} is mandatory. If clientid is absent, a "bad_request" will be returned.
- {username} and {password} are optional.
- if {username} or {password} is incorrect, the error code `unauthorized` will be returned.
- coap client should do keepalive work periodically to keep mqtt session online, especially those devices in a NAT network.


CoAP Client NOTES
-----------------
emqx-coap gateway does not accept POST and DELETE requests.

Topics in URI should be percent-encoded, but corresponding uri_path option has percent-encoding converted. Please refer to RFC 7252 section 6.4, "Decomposing URIs into Options":

> Note that these rules completely resolve any percent-encoding.

That implies coap client is responsible to convert any percert-encoding into true character while assembling coap packet.


ClientId, Username, Password and Topic
--------------------------------------
ClientId/username/password/topic in the coap URI are the concepts in mqtt. That is to say, emqx-coap is trying to fit coap message into mqtt system, by borrowing the client/username/password/topic from mqtt.

The Auth/ACL/Hook features in mqtt also applies on coap stuff. For example:
- If username/password is not authorized, coap client will get an unauthorized error.
- If username or clientid is not allowed to published specific topic, coap message will be dropped in fact, although coap client will get an acknoledgement from emqx-coap.
- If a coap message is published, a 'message.publish' hook is able to capture this message as well.

well-known locations
--------------------
Discovery always return "</mqtt>,</ps>"

For example
```
libcoap/examples/coap-client -m get "coap://127.0.0.1/.well-known/core"
```

License
-------

Apache License Version 2.0

Author
------

EMQX Team.

