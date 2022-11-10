# EMQX Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg?branch=master)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

English | [简体中文](./README-CN.md) | [日本語](./README-JP.md) | [русский](./README-RU.md)

*EMQX* broker is a fully open source, highly scalable, highly available distributed MQTT messaging broker for IoT, M2M and Mobile applications that can handle tens of millions of concurrent clients.

Starting from 3.0 release, *EMQX* broker fully supports MQTT V5.0 protocol specifications and backward compatible with MQTT V3.1 and V3.1.1,  as well as other communication protocols such as MQTT-SN, CoAP, LwM2M, WebSocket and STOMP. The 3.0 release of the *EMQX* broker can scaled to 10+ million concurrent MQTT connections on one cluster.

- For full list of new features, please read [EMQX Release Notes](https://github.com/emqx/emqx/releases).
- For more information, please visit [EMQX homepage](https://www.emqx.io).

## Installation

The *EMQX* broker is cross-platform, which supports Linux, Unix, macOS and Windows. It means *EMQX* can be deployed on x86_64 architecture servers and ARM devices like Raspberry Pi.

See more details for building and running *EMQX* on Windows in [Windows.md](./Windows.md)

#### Installing via EMQX Docker Image

```
docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### Installing via Binary Package

Get the binary package of the corresponding OS from [EMQX Download](https://www.emqx.io/downloads) page.

- [Single Node Install](https://docs.emqx.io/en/broker/latest/getting-started/install.html)
- [Multi Node Install](https://docs.emqx.io/en/broker/latest/advanced/cluster.html)


## Build From Source

The *EMQX* broker requires Erlang/OTP R21+ to build since 3.0 release.

For 4.3 and later versions.

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin console
```

For earlier versions, release has to be built from another repo.

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## Quick Start

If emqx is built from source, `cd _build/emqx/rel/emqx`.
Or change to the installation root directory if emqx is installed from a release package.

```bash
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

To view the dashboard after running, use your browser to open: http://localhost:18083

## Test

### To test everything in one go

```
make eunit ct
```

### To run subset of the common tests

Examples

```bash
make apps/emqx_bridge_mqtt-ct
```

### Dialyzer
##### To Analyze all the apps
```
make dialyzer
```

##### To Analyse specific apps, (list of comma separated apps)
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_auth_jwt,emqx_auth_ldap make dialyzer
```

## Community

### FAQ

Visiting [EMQX FAQ](https://docs.emqx.io/en/broker/latest/faq/faq.html) to get help of common problems.


### Questions

[GitHub Discussions](https://github.com/emqx/emqx/discussions) is where you can ask questions, and share ideas.

### Proposals

For more organised improvement proposals, you can send pull requests to [EIP](https://github.com/emqx/eip).

### Plugin development

To develop your own plugins, see [lib-extra/README.md](./lib-extra/README.md)


## MQTT Specifications

You can read the mqtt protocol via the following links:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## License

Apache License 2.0, see [LICENSE](./LICENSE).
