
## Overview [![Build Status](https://travis-ci.org/emqtt/emqttd.svg?branch=master)](https://travis-ci.org/emqtt/emqttd)

emqttd is a massively scalable and clusterable MQTT V3.1/V3.1.1 broker written in Erlang/OTP. emqttd support both MQTT V3.1/V3.1.1 protocol specification with extended features.

emqttd requires Erlang R17+ to build.

## Goals

emqttd is aimed to provide a solid, enterprise grade, extensible open-source MQTT broker for IoT(M2M) applications that need to support ten millions of concurrent MQTT clients.

## Features

* Full MQTT V3.1/V3.1.1 protocol specification support
* QoS0, QoS1, QoS2 Publish and Subscribe
* Session Management and Offline Messages
* Retained Messages Support
* Last Will Message Support
* TCP/SSL Connection Support
* MQTT Over Websocket(SSL) Support
* HTTP Publish API Support
* [$SYS/borkers/#](https://github.com/emqtt/emqtt/wiki/$SYS-Topics-of-Broker) Support
* Client Authentication with clientId, ipaddress
* Client Authentication with username, password.
* Client ACL control with ipaddress, clientid, username.
* Cluster brokers on several servers.
* Bridge brokers locally or remotelly
* 500K+ concurrent clients connections per server
* Extensible architecture with plugin support
* Passed eclipse paho interoperability tests

## Design

![emqttd architecture](http://emqtt.io/static/img/Architecture.png)

## QuickStart

Download binary packeges for linux, mac and freebsd from [http://emqtt.io/downloads](http://emqtt.io/downloads).

For example:

```sh
tar xvf emqttd-ubuntu64-0.7.0-alpha.tgz && cd emqttd

# start console
./bin/emqttd console

# start as daemon
./bin/emqttd start

# check status
./bin/emqttd_ctl status

# stop
./bin/emqttd stop
``` 

Build from source:

```
git clone https://github.com/emqtt/emqttd.git

cd emqttd && make && make dist
```

## GetStarted

Read [emqtt wiki](https://github.com/emqtt/emqttd/wiki) for detailed installation and configuration guide.

## Benchmark

Benchmark 0.6.1-alpha on a ubuntu/14.04 server with 8 cores, 32G memory from QingCloud:

200K+ Connections, 200K+ Topics, 20K+ In/Out Messages/sec, 20Mbps+ In/Out with 8G Memory, 50%CPU/core

## License

The MIT License (MIT)

## Contributors

[@hejin1026](https://github.com/hejin1026)
[@desoulter](https://github.com/desoulter)
[@turtleDeng](https://github.com/turtleDeng)
[@Hades32](https://github.com/Hades32)

## Author

Feng Lee <feng@emqtt.io>

