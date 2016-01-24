
$SYS Topics
===========

$SYS Topics of broker is started with::

    $SYS/brokers/${node}

${node} is erlang node name of clustered brokers. For example::

    $SYS/brokers/emqttd@host1/version
    $SYS/brokers/emqttd@host2/version

Broker $SYS Topics
------------------

Topic                          | Description
-------------------------------|------------
$SYS/brokers                   | Broker nodes
$SYS/brokers/${node}/version   | Broker Version
$SYS/brokers/${node}/uptime    | Broker Uptime
$SYS/brokers/${node}/datetime  | Broker DateTime
$SYS/brokers/${node}/sysdescr  | Broker Description
 
Client $SYS Topics
------------------

Start with: $SYS/brokers/${node}/clients/

Topic                 |   Payload(json)     | Description
----------------------|---------------------|--------------- 
${clientid}/connected | {ipaddress: "127.0.0.1", username: "test", session: false, version: 3, connack: 0, ts: 1432648482} | Publish when client connected 
${clientid}/disconnected | {reason: "normal" | "keepalive_timeout" | "conn_closed"}

Parameters of 'connected' Payload::

    ipaddress: "127.0.0.1", 
    username: "test", 
    session: false, 
    protocol: 3, 
    connack: 0, 
    ts: 1432648482

Parameters of 'disconnected' Payload::

    reason: normal,
    ts: 1432648486

Statistics $SYS Topics
----------------------

Start with '$SYS/brokers/${node}/stats/'

Client Stats
----------------------

Topic                                | Description
-------------------------------------|------------
clients/count   | count of current connected clients
clients/max     | max connected clients in the same time

Session Stats
----------------------

Topic            | Description
-----------------|------------
sessions/count   | count of current sessions
sessions/max     | max number of sessions

Subscriber Stats
----------------------

Topic             | Description
------------------|------------
subscriptions/count | count of current subscriptions
subscriptions/max   | max number of subscriptions

Topic Stats
----------------------

Topic             | Description
------------------|------------
topics/count      | count of current topics
topics/max        | max number of topics

Queue Stats
----------------------

Topic             | Description
------------------|------------
queues/count      | count of current queues
queues/max        | max number of queues


Metrics $SYS Topics
----------------------

Start with '$SYS/brokers/${node}/metrics/'

Bytes sent and received
----------------------

Topic                               | Description
------------------------------------|------------
bytes/received | MQTT Bytes Received since broker started
bytes/sent     | MQTT Bytes Sent since the broker started

Packets sent and received
-------------------------
 
Topic                    | Description
-------------------------|------------
packets/received         | MQTT Packets received
packets/sent             | MQTT Packets sent
packets/connect          | MQTT CONNECT Packet received
packets/connack          | MQTT CONNACK Packet sent
packets/publish/received | MQTT PUBLISH packets received
packets/publish/sent     | MQTT PUBLISH packets sent
packets/subscribe        | MQTT SUBSCRIBE Packets received
packets/suback           | MQTT SUBACK packets sent
packets/unsubscribe      | MQTT UNSUBSCRIBE Packets received
packets/unsuback         | MQTT UNSUBACK Packets sent
packets/pingreq          | MQTT PINGREQ packets received
packets/pingresp         | MQTT PINGRESP Packets sent
packets/disconnect       | MQTT DISCONNECT Packets received

Messages sent and received
---------------------------

Topic                                  | Description
---------------------------------------|-------------------
messages/received | Messages Received
messages/sent     | Messages Sent
messages/retained | Messages Retained
messages/stored   | TODO: Messages Stored
messages/dropped  | Messages Dropped

Alarm Topics
---------------------------

Start with '$SYS/brokers/${node}/alarms/'

Topic            | Description
-----------------|-------------------
${alarmId}/alert | New Alarm
${alarmId}/clear | Clear Alarm

Logs
---------------------------

'$SYS/brokers/${node}/logs/${severity}'

Severity   |  Description
-----------|-------------------
debug      | Debug Log
info       | Info Log
notice     | Notice Log
warning    | Warning Log
error      | Error Log
critical   | Critical Log

Sysmon
---------------------------

Start with '$SYS/brokers/${node}/sysmon/'

Topic            | Description
-----------------|-------------------
long_gc          | Long GC Warning
long_schedule    | Long Schedule
large_heap       | Large Heap Warning
busy_port        | Busy Port Warning
busy_dist_port   | Busy Dist Port

Logs(TODO)
---------------------------

'$SYS/brokers/${node}/log/${severity}'

Severity    | Description
------------|-------------------
debug       | Debug
info        | Info Log
notice      | Notice Log
warning     | Warning Log
error       | Error Log
critical    | Critical Log
alert       | Alert Log

VM Load Topics(TODO)
---------------------------

Start with '$SYS/brokers/${node}/vm/'

Topic            | Description
-----------------|-------------------
memory/*         | TODO
cpu/*            | TODO
processes/*      | TODO

Sys Interval
---------------------------

sys_interval: 1 minute default

