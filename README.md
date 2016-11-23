
## Overview [![Build Status](https://travis-ci.org/emqtt/emqttd.svg?branch=master)](https://travis-ci.org/emqtt/emqttd)

emqttd is a massively scalable and clusterable MQTT V3.1/V3.1.1 broker written in Erlang/OTP.

emqttd is fully open source and licensed under the Apache Version 2.0. emqttd implements both MQTT V3.1 and V3.1.1 protocol specifications, and supports WebSocket, STOMP, SockJS, CoAP and MQTT-SN at the same time.

emqttd requires Erlang R18+ to build since 1.1 release.

Follow us on Twitter: [@emqtt](https://twitter.com/emqtt)

## Cluster

The **q.emqtt.com** hosts a public emqttd cluster on [QingCloud](https://qingcloud.com):

![qing_cluster](http://emqtt.io/static/img/public_cluster.png)

## Goals

The emqttd project is aimed to implement a scalable, distributed, extensible open-source MQTT broker for IoT, M2M and Mobile applications that hope to handle millions of concurrent MQTT clients.

* Easy to install
* Massively scalable
* Easy to extend
* Solid stable

## Features

* Full MQTT V3.1/V3.1.1 protocol specification support
* QoS0, QoS1, QoS2 Publish and Subscribe
* Session Management and Offline Messages
* Retained Message
* Last Will Message
* TCP/SSL Connection
* MQTT Over WebSocket(SSL)
* HTTP Publish API
* MQTT-SN Protocol
* STOMP protocol
* STOMP over SockJS
* $SYS/# Topics
* ClientID Authentication
* IpAddress Authentication
* Username and Password Authentication
* Access control based on IpAddress, ClientID, Username
* Authentication with LDAP, Redis, MySQL, PostgreSQL and HTTP API
* Cluster brokers on several servers
* Bridge brokers locally or remotely
* mosquitto, RSMB bridge
* Extensible architecture with Hooks, Modules and Plugins
* Passed eclipse paho interoperability tests
* Local Subscription
* Shared Subscription

## Plugins

Plugin                                                                 | Description
-----------------------------------------------------------------------|--------------------------------------
[emq_plugin_template](https://github.com/emqtt/emq_plugin_template)    | Plugin template and demo
[emq_dashboard](https://github.com/emqtt/emq_dashboard)                | Web Dashboard
[emq_auth_username](https://github.com/emqtt/emq_auth_username)        | Username/Password Authentication Plugin
[emq_auth_clientid](https://github.com/emqtt/emq_auth_clientid)        | ClientId Authentication Plugin
[emq_auth_mysql](https://github.com/emqtt/emq_auth_mysql)              | MySQL Authentication/ACL Plugin
[emq_auth_pgsql](https://github.com/emqtt/emq_auth_pgsql)              | PostgreSQL Authentication/ACL Plugin
[emq_auth_redis](https://github.com/emqtt/emq_auth_redis)              | Redis Authentication/ACL Plugin
[emq_auth_mongo](https://github.com/emqtt/emq_auth_mongo)              | MongoDB Authentication/ACL Plugin
[emq_auth_http](https://github.com/emqtt/emq_auth_http)                | Authentication/ACL by HTTP API
[emq_auth_ldap](https://github.com/emqtt/emq_auth_ldap)                | LDAP Authentication Plugin
[emq_mod_presence](https://github.com/emqtt/emq_mod_presence)          | Presence Module
[emq_mod_rewrite](https://github.com/emqtt/emq_mod_rewrite)            | Rewrite Module
[emq_mod_retainer](https://github.com/emqtt/emq_mod_retainer)          | Store MQTT Retained Messages
[emq_mod_subscription](https://github.com/emqtt/emq_mod_subscription)  | Subscribe topics when client connected
[emq_sn](https://github.com/emqtt/emq_sn)                              | MQTT-SN Protocol Plugin
[emq_coap](https://github.com/emqtt/emq_coap)                          | CoAP Protocol Plugin
[emq_stomp](https://github.com/emqtt/emq_stomp)                        | Stomp Protocol Plugin
[emq_recon](https://github.com/emqtt/emq_recon)                        | Recon Plugin
[emq_reloader](https://github.com/emqtt/emq_reloader)                  | Reloader Plugin
[emq_sockjs](https://github.com/emqtt/emq_sockjs)                      | SockJS(Stomp) Plugin

## Dashboard

A Web Dashboard will be loaded when the emqttd broker started successfully.

The Dashboard helps monitor broker's running status, statistics and metrics of MQTT packets.

Default Address: http://localhost:18083

Default Login/Password: admin/public

## Design

![emqttd architecture](http://emqtt.io/static/img/architecture.png)

## QuickStart

Download binary package for Linux, Mac and Freebsd from [http://emqtt.io/downloads](http://emqtt.io/downloads).

Installing on Ubuntu64, for example:

```sh
unzip emqttd-ubuntu64-2.0-rc.2-20161019.zip && cd emqttd

# start console
./bin/emqttd console

# start as daemon
./bin/emqttd start

# check status
./bin/emqttd_ctl status

# stop
./bin/emqttd stop
``` 

Installing from source:

```
git clone https://github.com/emqtt/emq-relx.git

cd emq-relx && make

cd _rel/emqttd && ./bin/emqttd console
```

## Documents

Read Documents on [emqttd-docs.rtfd.org](http://emqttd-docs.rtfd.org) for installation and configuration guide.

## Benchmark

Latest release of emqttd broker is scaling to 1.3 million MQTT connections on a 12 Core, 32G CentOS server.

Benchmark 0.12.0-beta on a CentOS6 server with 8 Core, 32G memory from QingCloud:

250K Connections, 250K Topics, 250K Subscriptions, 4K Qos1 Messages/Sec In, 20K Qos1 Messages/Sec Out, 8M+(bps) In, 40M+(bps) Out Traffic

Consumed  about 3.6G memory and 400+% CPU.

Benchmark Report: [benchmark for 0.12.0 release](https://github.com/emqtt/emqttd/wiki/benchmark-for-0.12.0-release)

## Supports

* Twitter: [@emqtt](https://twitter.com/emqtt)
* Homepage: http://emqtt.io
* Downloads: http://emqtt.io/downloads
* Wiki: https://github.com/emqtt/emqttd/wiki
* Forum: https://groups.google.com/d/forum/emqtt
* Mailing List: <emqtt@googlegroups.com>
* Issues: https://github.com/emqtt/emqttd/issues
* QQ Group: 12222225

## Contributors

* [@callbay](https://github.com/callbay)
* [@lsxredrain](https://github.com/lsxredrain)
* [@hejin1026](https://github.com/hejin1026)
* [@desoulter](https://github.com/desoulter)
* [@turtleDeng](https://github.com/turtleDeng)
* [@Hades32](https://github.com/Hades32)
* [@huangdan](https://github.com/huangdan)
* [@phanimahesh](https://github.com/phanimahesh)
* [@dvliman](https://github.com/dvliman)

## Partners

[QingCloud](https://qingcloud.com) is the world’s first IaaS provider that can deliver any number of IT resources in seconds and adopts a second-based billing system. QingCloud is committed to providing a reliable, secure, on-demand and real-time IT resource platform with excellent performance, which includes all components of a complete IT infrastructure system: computing, storage, networking and security.

## Author

Feng Lee <feng@emqtt.io>

## License

Apache License Version 2.0

