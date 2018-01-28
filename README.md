
# *EMQ* - Erlang MQTT Broker

[![Build Status](https://travis-ci.org/emqtt/emqttd.svg?branch=master)](https://travis-ci.org/emqtt/emqttd)

*EMQ* (Erlang MQTT Broker) is a distributed, massively scalable, highly extensible MQTT message broker written in Erlang/OTP.

*EMQ* is fully open source and licensed under the Apache Version 2.0. *EMQ* implements both MQTT V3.1 and V3.1.1 protocol specifications, and supports MQTT-SN, CoAP, WebSocket, STOMP and SockJS at the same time.

*EMQ* provides a scalable, reliable, enterprise-grade MQTT message Hub for IoT, M2M, Smart Hardware and Mobile Messaging Applications.

The 1.0 release of the EMQ broker has scaled to 1.3 million concurrent MQTT connections on a 12 Core, 32G CentOS server.

Please visit [emqtt.io](http://emqtt.io) for more service. Follow us on Twitter: [@emqtt](https://twitter.com/emqtt)

## Features

* Full MQTT V3.1/V3.1.1 support
* QoS0, QoS1, QoS2 Publish/Subscribe
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
* JWT Authentication
* LDAP Authentication/ACL
* HTTP Authentication/ACL
* MySQL Authentication/ACL
* Redis Authentication/ACL
* PostgreSQL Authentication/ACL
* MongoDB Authentication/ACL
* Cluster brokers on several nodes 
* Bridge brokers locally or remotely
* mosquitto, RSMB bridge
* Extensible architecture with Hooks and Plugins
* Passed eclipse paho interoperability tests
* Local Subscription
* Shared Subscription
* Proxy Protocol V1/2
* Lua Hook and Web Hook
* LWM2M Prototol Support

## Installation

The *EMQ* broker is cross-platform, which can be deployed on Linux, Unix, Mac, Windows and even Raspberry Pi.

Download the binary package for your platform from http://emqtt.io/downloads.

Documentation on [emqtt.io/docs/v2/](http://emqtt.io/docs/v2/install.html), [docs.emqtt.com](http://docs.emqtt.com/en/latest/install.html) for installation and configuration guide.

## Build From Source

The *EMQ* broker requires Erlang/OTP R19+ to build since 2.1 release.

```
git clone https://github.com/emqtt/emq-relx.git

cd emq-relx && make

cd _rel/emqttd && ./bin/emqttd console
```

## Plugins

The *EMQ* broker is highly extensible, with many hooks and plugins for customizing the authentication/ACL and integrating with other systems:

Plugin                                                                 | Description
-----------------------------------------------------------------------|--------------------------------------
[emq_plugin_template](https://github.com/emqtt/emq_plugin_template)    | Plugin template and demo
[emq_dashboard](https://github.com/emqtt/emq_dashboard)                | Web Dashboard
[emq_retainer](https://github.com/emqtt/emq-retainer)                  | Store MQTT Retained Messages
[emq_modules](https://github.com/emqtt/emq-modules)                    | Presence, Subscription and Rewrite Modules
[emq_auth_username](https://github.com/emqtt/emq_auth_username)        | Username/Password Authentication Plugin
[emq_auth_clientid](https://github.com/emqtt/emq_auth_clientid)        | ClientId Authentication Plugin
[emq_auth_mysql](https://github.com/emqtt/emq_auth_mysql)              | MySQL Authentication/ACL Plugin
[emq_auth_pgsql](https://github.com/emqtt/emq_auth_pgsql)              | PostgreSQL Authentication/ACL Plugin
[emq_auth_redis](https://github.com/emqtt/emq_auth_redis)              | Redis Authentication/ACL Plugin
[emq_auth_mongo](https://github.com/emqtt/emq_auth_mongo)              | MongoDB Authentication/ACL Plugin
[emq_auth_http](https://github.com/emqtt/emq_auth_http)                | Authentication/ACL by HTTP API
[emq_auth_ldap](https://github.com/emqtt/emq_auth_ldap)                | LDAP Authentication Plugin
[emq_auth_jwt](https://github.com/emqtt/emq-auth-jwt)                  | JWT Authentication Plugin
[emq_web_hook](https://github.com/emqtt/emq-web-hook)                  | Web Hook Plugin
[emq_lua_hook](https://github.com/emqtt/emq-lua-hook)                  | Lua Hook Plugin
[emq_sn](https://github.com/emqtt/emq_sn)                              | MQTT-SN Protocol Plugin
[emq_coap](https://github.com/emqtt/emq_coap)                          | CoAP Protocol Plugin
[emq_stomp](https://github.com/emqtt/emq_stomp)                        | Stomp Protocol Plugin
[emq_lwm2m](https://github.com/emqx/emqx-lwm2m)                        | LWM2M Prototol Plugin
[emq_recon](https://github.com/emqtt/emq_recon)                        | Recon Plugin
[emq_reloader](https://github.com/emqtt/emq_reloader)                  | Reloader Plugin
[emq_sockjs](https://github.com/emqtt/emq_sockjs)                      | SockJS(Stomp) Plugin

## Supports

* Twitter: [@emqtt](https://twitter.com/emqtt)
* Homepage: http://emqtt.io
* Downloads: http://emqtt.io/downloads
* Documentation: http://emqtt.io/docs/v2/
* Forum: https://groups.google.com/d/forum/emqtt
* Mailing List: <emqtt@googlegroups.com>
* Issues: https://github.com/emqtt/emqttd/issues
* QQ Group: 12222225

## Test Servers

The **q.emqtt.com** hosts a public Four-Node *EMQ* cluster on [QingCloud](https://qingcloud.com):

![qing_cluster](http://emqtt.io/static/img/public_cluster.png)

## License

Apache License Version 2.0

