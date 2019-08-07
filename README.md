# EMQ X Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases) 
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx) 
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg)](https://coveralls.io/github/emqx/emqx) 
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx) 
[![Slack](https://img.shields.io/badge/Slack-EMQ%20X-brightgreen?logo=slack&style=flat&color=7E4798)](https://emqx.slack.com)  
[![Twitter](https://img.shields.io/twitter/follow/emqtt?label=EMQ%20X&style=social&color=1DA1F2)](https://twitter.com/emqtt)

English | [简体中文](./README-CN.md)

*EMQ X* broker is a fully open source, highly scalable, highly available distributed MQTT messaging broker for IoT, M2M and Mobile applications that can handle tens of millions of concurrent clients.

Starting from 3.0 release, *EMQ X* broker fully supports MQTT V5.0 protocol specifications and backward compatible with MQTT V3.1 and V3.1.1,  as well as other communication protocols such as MQTT-SN, CoAP, LwM2M, WebSocket and STOMP. The 3.0 release of the *EMQ X* broker can scaled to 10+ million concurrent MQTT connections on one cluster.


- For full list of new features, please read *EMQ X* broker 3.0 [release notes](https://github.com/emqx/emqx/releases/).
- For more information, please visit [EMQ X homepage](http://emqx.io).


## Installation

The *EMQ X* broker is cross-platform, which can be deployed on Linux, Unix, Mac, Windows and even Raspberry Pi.

Download the binary package for your platform from [here](http://emqx.io/downloads).

- [Single Node Install](https://developer.emqx.io/docs/emq/v3/en/install.html)
- [Multi Node Install](https://developer.emqx.io/docs/emq/v3/en/cluster.html)


## Build From Source

The *EMQ X* broker requires Erlang/OTP R21+ to build since 3.0 release.

```
git clone https://github.com/emqx/emqx-rel.git

cd emqx-rel && make

cd _rel/emqx && ./bin/emqx console

```

## Quick Start

    # Start emqx
    ./bin/emqx start

    # Check Status
    ./bin/emqx_ctl status

    # Stop emqx
    ./bin/emqx stop

  To view the dashboard after running, use your browser to open: http://localhost:18083


## Roadmap

The [EMQ X Roadmap uses Github milestones](https://github.com/emqx/emqx/milestones) to track the progress of the project.

## Community, discussion, contribution, and support

You can reach the EMQ community and developers via the following channels:
- [EMQX Slack](http://emqx.slack.com)
   -[#emqx-users](https://emqx.slack.com/messages/CBUF2TTB8/)
   -[#emqx-devs](https://emqx.slack.com/messages/CBSL57DUH/)
- [Mailing Lists](<emqtt@googlegroups.com>)
- [Twitter](https://twitter.com/emqtt)
- [Forum](https://groups.google.com/d/forum/emqtt)
- [Blog](https://medium.com/@emqtt)

Please submit any bugs, issues, and feature requests to [emqx/emqx](https://github.com/emqx/emqx/issues).

## MQTT Specifications

You can read the mqtt protocol via the following links:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## License

Copyright (c) 2013-2019 [EMQ Technologies Co., Ltd](http://emqx.io). All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");you may not use this file except in compliance with the License.You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
