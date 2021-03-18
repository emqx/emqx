# EMQ X Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg?branch=master)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack Invite](<https://slack-invite.emqx.io/badge.svg>)](https://slack-invite.emqx.io)
[![Twitter](https://img.shields.io/badge/Follow-EMQ%20X-1DA1F2?logo=twitter)](https://twitter.com/emqtt)
[![Community](https://img.shields.io/badge/Community-EMQ%20X-yellow?logo=github)](https://github.com/emqx/emqx/discussions)

[![The best IoT MQTT open source team looks forward to your joining](https://www.emqx.io/static/img/github_readme_en_bg.png)](https://www.emqx.io/careers)

English | [简体中文](./README-CN.md) | [日本語](./README-JP.md)

*EMQ X* broker is a fully open source, highly scalable, highly available distributed MQTT messaging broker for IoT, M2M and Mobile applications that can handle tens of millions of concurrent clients.

Starting from 3.0 release, *EMQ X* broker fully supports MQTT V5.0 protocol specifications and backward compatible with MQTT V3.1 and V3.1.1,  as well as other communication protocols such as MQTT-SN, CoAP, LwM2M, WebSocket and STOMP. The 3.0 release of the *EMQ X* broker can scaled to 10+ million concurrent MQTT connections on one cluster.

- For full list of new features, please read [EMQ X Release Notes](https://github.com/emqx/emqx/releases).
- For more information, please visit [EMQ X homepage](https://www.emqx.io).

## Installation

The *EMQ X* broker is cross-platform, which supports Linux, Unix, macOS and Windows. It means *EMQ X* can be deployed on x86_64 architecture servers and ARM devices like Raspberry Pi.

See more details for building and running *EMQ X* on Windows in [Windows.md](./Windows.md)

#### Installing via EMQ X Docker Image

```
docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### Installing via Binary Package

Get the binary package of the corresponding OS from [EMQ X Download](https://www.emqx.io/downloads) page.

- [Single Node Install](https://docs.emqx.io/en/broker/latest/getting-started/install.html)
- [Multi Node Install](https://docs.emqx.io/en/broker/latest/advanced/cluster.html)


## Build From Source

The *EMQ X* broker requires Erlang/OTP R21+ to build since 3.0 release.

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

If emqx is built from source, `cd _buid/emqx/rel/emqx`.
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

examples

```bash
./rebar3 ct --name 'test@127.0.0.1' -c -v --dir test,apps/emqx_sn,apps/emqx_coap
./rebar3 ct --name 'test@127.0.0.1' -c -v --dir apps/emqx_auth_mnesi --suite emqx_acl_mnesia_SUITE
./rebar3 ct --name 'test@127.0.0.1' -c -v --dir apps/emqx_auth_mnesi --suite emqx_acl_mnesia_SUITE --case t_rest_api
```

NOTE: Do *NOT* use full (relative) path to SUITE files like this `--suite apps/emqx_auth_mnesia/test/emqx_acl_mnesia_SUITE.erl`,
because it will lead to a full copy of `apps` dir into `_buid/test/lib/emqx`.

### Dialyzer
##### To Analyze all the apps
```
make dialyzer
```

##### To Analyse specific apps, (list of comma separated apps)
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_auth_jwt,emqx_auth_ldap make dialyzer
```

## FAQ

Visiting [EMQ X FAQ](https://docs.emqx.io/en/broker/latest/faq/faq.html) to get help of common problems.

## Roadmap

The [EMQ X Roadmap uses Github milestones](https://github.com/emqx/emqx/milestones) to track the progress of the project.

## Community

The EMQ X community can be found on [GitHub Discussions](https://github.com/emqx/emqx/discussions), where you can ask questions, voice ideas, and share your projects.

To chat with other community members you can join the [EMQ X Slack](https://slack-invite.emqx.io).

## MQTT Specifications

You can read the mqtt protocol via the following links:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## License

Apache License 2.0, see [LICENSE](./LICENSE).
