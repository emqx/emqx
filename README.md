# EMQ X Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg?branch=master)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack Invite](<https://slack-invite.emqx.io/badge.svg>)](https://slack-invite.emqx.io)
[![Twitter](https://img.shields.io/badge/Twitter-EMQ%20X-1DA1F2?logo=twitter)](https://twitter.com/emqtt)


[![We are hiring](https://www.emqx.io/static/img/github_readme_en_bg.png)](https://emqx.io/about/jobs)

English | [简体中文](./README-CN.md)

*EMQ X* broker is a fully open source, highly scalable, highly available distributed MQTT messaging broker for IoT, M2M and Mobile applications that can handle tens of millions of concurrent clients.

Starting from 3.0 release, *EMQ X* broker fully supports MQTT V5.0 protocol specifications and backward compatible with MQTT V3.1 and V3.1.1,  as well as other communication protocols such as MQTT-SN, CoAP, LwM2M, WebSocket and STOMP. The 3.0 release of the *EMQ X* broker can scaled to 10+ million concurrent MQTT connections on one cluster.

- For full list of new features, please read [EMQ X Release Notes](https://github.com/emqx/emqx/releases).
- For more information, please visit [EMQ X homepage](https://emqx.io).

## Installation

The *EMQ X* broker is cross-platform, which supports Linux, Unix, Mac OS and Windows. It means *EMQ X* can be deployed on x86_64 architecture servers and ARM devices like Raspberry Pi.

**Installing via EMQ X Docker Image**

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

**Or download the binary package for your platform from [here](https://emqx.io/downloads).**

- [Single Node Install](https://docs.emqx.io/broker/v3/en/install.html)
- [Multi Node Install](https://docs.emqx.io/broker/v3/en/cluster.html)


## Build From Source

The *EMQ X* broker requires Erlang/OTP R21+ to build since 3.0 release.

```
git clone -b v4.0.0 https://github.com/emqx/emqx-rel.git

cd emqx-rel && make

cd _build/emqx/rel/emqx && ./bin/emqx console

```

## Quick Start

```
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

To view the dashboard after running, use your browser to open: http://localhost:18083

## FAQ

Visiting [FAQ](https://docs.emqx.io/tutorial/v3/en/faq/faq.html) to get help of common problems.

## Roadmap

The [EMQ X Roadmap uses Github milestones](https://github.com/emqx/emqx/milestones) to track the progress of the project.

## Community, discussion, contribution, and support

You can reach the EMQ community and developers via the following channels:
- [EMQX Slack](https://slack-invite.emqx.io/)
- [Twitter](https://twitter.com/emqtt)
- [Forum](https://groups.google.com/d/forum/emqtt)
- [Blog](https://medium.com/@emqtt)
- [Reddit](https://www.reddit.com/r/emqx/)

Please submit any bugs, issues, and feature requests to [emqx/emqx](https://github.com/emqx/emqx/issues).

## MQTT Specifications

You can read the mqtt protocol via the following links:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## License

Apache License 2.0, see [LICENSE](./LICENSE).
