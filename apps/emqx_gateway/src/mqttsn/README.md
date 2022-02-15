# MQTT-SN Gateway

EMQX MQTT-SN Gateway.

## Configure Plugin


File: etc/emqx_sn.conf

```
## The UDP port which emq-sn is listening on.
##
## Value: IP:Port | Port
##
## Examples: 1884, 127.0.0.1:1884, ::1:1884
mqtt.sn.port = 1884

## The duration(seconds) that emq-sn broadcast ADVERTISE message through.
##
## Value: Second
mqtt.sn.advertise_duration = 900

## The MQTT-SN Gateway id in ADVERTISE message.
##
## Value: Number
mqtt.sn.gateway_id = 1

## To control whether write statistics data into ETS table for dashboard to read.
##
## Value: on | off
mqtt.sn.enable_stats = off

## To control whether accept and process the received publish message with qos=-1.
##
## Value: on | off
mqtt.sn.enable_qos3 = off

## The pre-defined topic name corresponding to the pre-defined topic id of N.
## Note that the pre-defined topic id of 0 is reserved.
mqtt.sn.predefined.topic.0 = reserved
mqtt.sn.predefined.topic.1 = /predefined/topic/name/hello
mqtt.sn.predefined.topic.2 = /predefined/topic/name/nice

## Default username for MQTT-SN. This parameter is optional. If specified,
## emq-sn will connect EMQ core with this username. It is useful if any auth
## plug-in is enabled.
##
## Value: String
mqtt.sn.username = mqtt_sn_user

## This parameter is optional. Pair with username above.
##
## Value: String
mqtt.sn.password = abc
```

- mqtt.sn.port
  * The UDP port which emqx-sn is listening on.
- mqtt.sn.advertise_duration
  * The duration(seconds) that emqx-sn broadcast ADVERTISE message through.
- mqtt.sn.gateway_id
  * Gateway id in ADVERTISE message.
- mqtt.sn.enable_stats
  * To control whether write statistics data into ETS table for dashboard to read.
- mqtt.sn.enable_qos3
  * To control whether accept and process the received publish message with qos=-1.
- mqtt.sn.predefined.topic.N
  * The pre-defined topic name corresponding to the pre-defined topic id of N. Note that the pre-defined topic id of 0 is reserved.
- mqtt.sn.username
  * This parameter is optional. If specified, emqx-sn will connect EMQX core with this username. It is useful if any auth plug-in is enabled.
- mqtt.sn.password
  * This parameter is optional. Pair with username above.

## Load Plugin

```
./bin/emqx_ctl plugins load emqx_sn
```

## Client

### NOTE
- Topic ID is per-client, and will be cleared if client disconnected with broker or keepalive failure is detected in broker.
- Please register your topics again each time connected with broker.
- If your udp socket(mqtt-sn client) has successfully connected to broker, don't try to send another CONNECT on this socket again, which will lead to confusing behaviour. If you want to start from beging, please do as following:
    + destroy your present socket and create a new socket to connect again
    + or send DISCONNECT on the same socket and connect again.

### Library

- https://github.com/eclipse/paho.mqtt-sn.embedded-c/
- https://github.com/ty4tw/MQTT-SN
- https://github.com/njh/mqtt-sn-tools
- https://github.com/arobenko/mqtt-sn

### sleeping device

PINGREQ must have a ClientId which is identical to the one in CONNECT message. Without ClientId, emqx-sn will ignore such PINGREQ.

### pre-defined topics

The mapping of a pre-defined topic id and topic name should be known inadvance by both client's application and gateway. We define this mapping info in emqx_sn.conf file, and which shall be kept equivalent in all client's side.

## License

Apache License Version 2.0

## Author

EMQX Team.
