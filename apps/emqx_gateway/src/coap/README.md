
# Table of Contents

1.  [EMQX 5.0 CoAP Gateway](#org6feb6de)
2.  [CoAP Message Processing Flow](#org8458c1a)
    1.  [Request Timing Diagram](#orgeaa4f53)
        1.  [Transport && Transport Manager](#org88207b8)
        2.  [Resource](#orgb32ce94)
3.  [Resource](#org8956f90)
    1.  [MQTT Resource](#orge8c21b1)
    2.  [PubSub Resource](#org68ddce7)
4.  [Heartbeat](#orgffdfecd)
5.  [Command](#org43004c2)
6.  [MQTT QOS <=> CoAP non/con](#org0157b5c)



<a id="org6feb6de"></a>

# EMQX 5.0 CoAP Gateway

emqx-coap is a CoAP Gateway for EMQ X Broker. 
It translates CoAP messages into MQTT messages and make it possible to communiate between CoAP clients and MQTT clients.


<a id="org8458c1a"></a>

# CoAP Message Processing Flow


<a id="orgeaa4f53"></a>

## Request Timing Diagram


    ,------.          ,------------.          ,-----------------.          ,---------.          ,--------.
    |client|          |coap_gateway|          |transport_manager|          |transport|          |resource|
    `--+---'          `-----+------'          `--------+--------'          `----+----'          `---+----'
       |                    |                          |                        |                   |
       | ------------------->                          |                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    | ------------------------>|                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |----------------------->|                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |------------------>|
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |<------------------|
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |<-----------------------|                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       |                    | <------------------------|                        |                   |
       |                    |                          |                        |                   |
       |                    |                          |                        |                   |
       | <-------------------                          |                        |                   |
    ,--+---.          ,-----+------.          ,--------+--------.          ,----+----.          ,---+----.
    |client|          |coap_gateway|          |transport_manager|          |transport|          |resource|
    `------'          `------------'          `-----------------'          `---------'          `--------'


<a id="org88207b8"></a>

### Transport && Transport Manager

Transport is a module that manages the life cycle and behaviour of CoAP messages\
And the transport manager is to manage all transport which in this gateway


<a id="orgb32ce94"></a>

### Resource

The Resource is a behaviour that must implement GET/PUT/POST/DELETE method\
Different Resources can have different implementations of this four method\
Each gateway can only use one Resource module to process CoAP Request Message


<a id="org8956f90"></a>

# Resource


<a id="orge8c21b1"></a>

## MQTT Resource

The MQTT Resource is a simple CoAP to MQTT adapter, the implementation of each method is as follows:

-   use uri path as topic
-   GET: subscribe the topic
-   PUT: publish message to this topic
-   POST: like PUT
-   DELETE: unsubscribe the topic


<a id="org68ddce7"></a>

## PubSub Resource

The PubSub Resource like the MQTT Resource, but has a retained topic's message database\
This Resource is shared, only can has one instance. The implementation:

-   use uri path as topic
-   GET:
    -   GET with observe = 0: subscribe the topic
    -   GET with observe = 1: unsubscribe the topic
    -   GET without observe: read lastest message from the message database, key is the topic
-   PUT:
    insert message into the message database, key is the topic
-   POST:
    like PUT, but will publish the message
-   DELETE:
    delete message from the database, key is topic


<a id="orgffdfecd"></a>

# Heartbeat

At present, the CoAP gateway only supports UDP/DTLS connection, don't support UDP over TCP and UDP over WebSocket.
Because UDP is connectionless, so the client needs to send heartbeat ping to the server interval. Otherwise, the server will close related resources
Use ****POST with empty uri path**** as a heartbeat ping

example: 
```
coap-client -m post coap://127.0.0.1
```

<a id="org43004c2"></a>

# Command

Command is means the operation which outside the CoAP protocol, like authorization
The Command format:

1.  use ****POST**** method
2.  uri path is empty
3.  query string is like ****action=comandX&argX=valuex&argY=valueY****

example:
1. connect:
```
coap-client -m post coap://127.0.0.1?action=connect&clientid=XXX&username=XXX&password=XXX
```
2. disconnect:
```
coap-client -m post coap://127.0.0.1?action=disconnect
```

<a id="org0157b5c"></a>

# MQTT QOS <=> CoAP non/con

CoAP gateway uses some options to control the conversion between MQTT qos and coap non/con:

1.notify_type
Control the type of notify messages when the observed object has changed.Can be:

-   non
-   con
-   qos
    in this value, MQTT QOS0 -> non, QOS1/QOS2 -> con

2.subscribe_qos
Control the qos of subscribe.Can be:

-   qos0
-   qos1
-   qos2
-   coap
    in this value, CoAP non -> qos0, con -> qos1

3.publish_qos
like subscribe_qos, but control the qos of the publish MQTT message

License
-------

Apache License Version 2.0

Author
------

EMQ X Team.
