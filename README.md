# EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://img.shields.io/travis/emqx/emqx?label=Build)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://img.shields.io/coveralls/github/emqx/emqx/master?label=Coverage)](https://coveralls.io/github/emqx/emqx?branch=master)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx?label=Docker%20Pulls)](https://hub.docker.com/r/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ%20X-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)



English | [简体中文](./README-CN.md) | [日本語](./README-JP.md) | [русский](./README-RU.md)

EMQX is the most scalable and popular open-source MQTT broker with a high performance that connects 100M+ IoT devices in 1 cluster at 1ms latency. Move and process millions of MQTT messages per second.

The EMQX v5.0 has been verified in [test scenarios](https://www.emqx.com/en/blog/reaching-100m-mqtt-connections-with-emqx-5-0) to scale to 100 million concurrent device connections, which is a critically important milestone for IoT designers. It also comes with plenty of exciting new features and huge performance improvements, including a more powerful rule engine, enhanced security management, Mria database extension, and much more to enhance the scalability of IoT applications.

During the last several years, EMQX has gained popularity among IoT companies and is used by more than 20,000 global users from over 50 countries, with more than 100 million IoT device connections supported worldwide.

For more information, please visit [EMQX homepage](https://www.emqx.io/).

## Get Started

#### EMQX Cloud

The simplest way to set up EMQX is to create a managed deployment with EMQX Cloud. You can [try EMQX Cloud for free](https://www.emqx.com/en/signup?continue=https%3A%2F%2Fcloud-intl.emqx.com%2Fconsole%2F), no credit card required.

#### Run EMQX using Docker

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx:latest
```

Or install EMQX Enterprise with a built-in license for ten connections that never expire.

```
docker run -d --name emqx-ee -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx-ee:latest
```

Next, please follow the [getting started guide](https://www.emqx.io/docs/en/v5.0/getting-started/getting-started.html#start-emqx) to tour the EMQX features.

#### More installation options

If you prefer to install and manage EMQX yourself, you can download the latest version from [www.emqx.io/downloads](https://www.emqx.io/downloads).

For more installation options, see the [EMQX installation documentation](https://www.emqx.io/docs/en/v5.0/deploy/install.html).

## Documentation

The EMQX documentation is available at [www.emqx.io/docs/en/latest/](https://www.emqx.io/docs/en/latest/).

The EMQX Enterprise documentation is available at [docs.emqx.com/en/](https://docs.emqx.com/en/).

## Contributing

Please see our [contributing.md](./CONTRIBUTING.md).

For more organised improvement proposals, you can send pull requests to [EIP](https://github.com/emqx/eip).

## Get Involved

- Follow [@EMQTech on Twitter](https://twitter.com/EMQTech).
- If you have a specific question, check out our [discussion forums](https://github.com/emqx/emqx/discussions).
- For general discussions, join us on the [official Discord](https://discord.gg/xYGf3fQnES) team.
- Keep updated on [EMQX YouTube](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q) by subscribing.

## Resources

- [MQTT client programming](https://www.emqx.com/en/blog/tag/mqtt-client-programming)

  A series of blogs to help developers get started quickly with MQTT in PHP, Node.js, Python, Golang, and other programming languages.

- [MQTT SDKs](https://www.emqx.com/en/mqtt-client-sdk)

  We have selected popular MQTT client SDKs in various programming languages and provided code examples to help you quickly understand the use of MQTT clients.

- [MQTT X](https://mqttx.app/)

  An elegant cross-platform MQTT 5.0 client tool that provides desktop, command line, and web to help you develop and debug MQTT services and applications faster.

- [Internet of Vehicles](https://www.emqx.com/en/blog/category/internet-of-vehicles)

  Build a reliable, efficient, and industry-specific IoV platform based on EMQ's practical experience, from theoretical knowledge such as protocol selection to practical operations like platform architecture design.

## Build From Source

The `master` branch is for the latest version 5 release, checkout branch `main-v4.3` for version 4.3 and `main-v4.4` for version 4.4.

EMQX requires OTP 22 or 23 for version 4.3, and OTP 24 for 4.4 and 5.0.

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin/emqx console
```

For 4.2 or earlier versions, release has to be built from another repo.

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## License

See [LICENSE](./LICENSE).
