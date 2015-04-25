
## Overview [![Build Status](https://travis-ci.org/emqtt/emqttd.svg?branch=master)](https://travis-ci.org/emqtt/emqttd)

emqttd is a massively scalable and clusterable MQTT V3.1/V3.1.1 broker written in Erlang/OTP. emqttd support both MQTT V3.1/V3.1.1 protocol specification with extended features.

emqttd requires Erlang R17+ to build.

TODO: architecture diagraph.

## Goals

emqttd aims to provide a solid, carrier-class MQTT broker that could support millions concurrent connections.

## Featues

* Full MQTT V3.1/V3.1.1 protocol specification support
* QoS0, QoS1, QoS2 Publish and Subscribe
* Session Management and Offline Messages
* Retained Messages
* TCP/SSL connection support
* MQTT Over Websocket
* HTTP Publish API
* [$SYS/borkers/#](https://github.com/emqtt/emqtt/wiki/$SYS-Topics-of-Broker) support
* Client Authentication with clientId or username, password.
* Client ACL control with ipaddress, clientid, username.
* Cluster brokers on several servers.
* Bridge brokers locally or remotelly
* 500K+ concurrent client connections per server
* Extensible architecture with plugin support
* Passed eclipse paho interoperability tests

## Getting Started

Download binary packeges for linux, mac and freebsd from [http://emqtt.io/downloads](http://emqtt.io/downloads)

TODO: Getting Started Doc...

## Benchmark

Benchmark 0.6.1-alpha on a ubuntu/14.04 server with 8 cores, 32G memory from QingCloud:

200K+ Connections, 200K+ Topics, 20K+ In/Out Messages/sec, 20Mbps+ In/Out with 8G Memory, 50%CPU/core

## Sponsors

...Sponse us...

## License

The MIT License (MIT)

## Author 

Feng Lee <feng@emqtt.io>

