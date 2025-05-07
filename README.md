English | [简体中文](./README-CN.md) | [Русский](./README-RU.md)

# EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml/badge.svg)](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![X](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=x)](https://x.com/EMQTech)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)


EMQX is the world's most scalable [MQTT broker](https://www.emqx.com/en/blog/the-ultimate-guide-to-mqtt-broker-comparison) with a high performance that connects 100M+ IoT devices in 1 cluster, while maintaining 1M message per second throughput and sub-millisecond latency.

EMQX supports multiple open standard protocols like MQTT, HTTP, QUIC, and WebSocket. It’s 100% compliant with MQTT 5.0 and 3.x standard, and secures bi-directional communication with MQTT over TLS/SSL and various authentication mechanisms.

With the built-in powerful SQL-based [rules engine](https://www.emqx.com/en/solutions/iot-rule-engine), EMQX can extract, filter, enrich and transform IoT data in real-time. In addition, it ensures high availability and horizontal scalability with a masterless distributed architecture, and provides ops-friendly user experience and great observability.

EMQX boasts more than 20K+ enterprise users across 60+ countries and regions, connecting 250M+ IoT devices worldwide, and is trusted by over 1000 customers in mission-critical scenarios of IoT, IIoT, connected vehicles, and more, including over 70 Fortune 500 companies like HPE, VMware, Verifone, SAIC Volkswagen, and Ericsson.

For more information, please visit [EMQX homepage](https://www.emqx.com/en).

## Get Started

#### Run EMQX in the Cloud

The simplest way to set up EMQX is to create a managed deployment with EMQX Cloud. You can [try EMQX Cloud for free](https://www.emqx.com/en/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud-intl.emqx.com/console/deployments/0?oper=new), no credit card required.

#### Run EMQX using Docker

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx-enterprise:latest
```

Next, please follow the [Install EMQX Using Docker](https://docs.emqx.com/en/emqx/latest/deploy/install-docker.html) guide for further instructions.

#### Run EMQX cluster on Kubernetes

Please consult official [EMQX Operator](https://docs.emqx.com/en/emqx-operator/latest/getting-started/getting-started.html) documentation for details.

#### More installation options

If you prefer to install and manage EMQX yourself, you can download the latest version from [the official site](https://www.emqx.com/en/downloads-and-install/enterprise).

For more installation options, see the [EMQX installation documentation](https://docs.emqx.com/en/emqx/latest/deploy/install.html)

## Documentation

The EMQX documentation is available at [docs.emqx.com/en/emqx/latest](https://docs.emqx.com/en/emqx/latest/).

The EMQX Cloud documentation is available at [docs.emqx.com/en/cloud/latest](https://docs.emqx.com/en/cloud/latest/).

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md).

For more organised improvement proposals, you can send pull requests to [EIP](https://github.com/emqx/eip).

## Get Involved

- Follow [@EMQTech on Twitter](https://twitter.com/EMQTech).
- Join our [Slack](https://slack-invite.emqx.io/).
- If you have a specific question, check out our [discussion forums](https://github.com/emqx/emqx/discussions).
- For general discussions, join us on the [official Discord](https://discord.gg/xYGf3fQnES) team.
- Keep updated on [EMQX YouTube](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q) by subscribing.

## Resources

- [MQTT client programming](https://www.emqx.com/en/blog/category/mqtt-programming)

  A series of blogs to help developers get started quickly with MQTT in PHP, Node.js, Python, Golang, and other programming languages.

- [MQTT SDKs](https://www.emqx.com/en/mqtt-client-sdk)

  We have selected popular MQTT client SDKs in various programming languages and provided code examples to help you quickly understand the use of MQTT clients.

- [MQTTX](https://mqttx.app/)

  An elegant cross-platform MQTT 5.0 client tool that provides desktop, command line, and web to help you develop and debug MQTT services and applications faster.

- [Internet of Vehicles](https://www.emqx.com/en/blog/category/internet-of-vehicles)

  Build a reliable, efficient, and industry-specific IoV platform based on EMQ's practical experience, from theoretical knowledge such as protocol selection to practical operations like platform architecture design.

## Build From Source

The `master` branch tracks the latest version 5. For version 4.4 checkout the `main-v4.4` branch.

* EMQX 4.4 requires OTP 24.
* EMQX 5.0 ~ 5.3 can be built with OTP 24 or 25.
* EMQX 5.4 and newer can be built with OTP 25 or 26.

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

## Rolling Upgrade Paths Since 5.0

Below is the matrix supported rolling upgrade paths since 5.0.

- Version numbers end with `?` e.g. `6.0?` are future releases.
- ✅: Supported, or planed to support.
- ⚠️:  May experience issues, require manual resolution.
- ❌: Not supported.
- 🔄: Tentative support for future versions.

See release notes for detailed information.

| From\To  | 5.1  | 5.2  | 5.3  | 5.4  | 5.5  | 5.6  | 5.7  | 5.8  | 5.9   | 5.10? | 6.0?  |
|----------|------|------|------|------|------|------|------|------|-------|-------|-------|
| 5.0      | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ⚠️[1]  | ❌[2] | ❌[2] |
| 5.1      | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅    | ❌[2] | ❌[2] |
| 5.2      |      | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅    | ❌[2] | ❌[2] |
| 5.3      |      |      | ✅   | ✅   | ✅   | ✅   | ✅   | ✅   | ✅    | ❌[2] | ❌[2] |
| 5.4      |      |      |      | ✅   | ✅   | ⚠️    | ✅   | ✅   | ✅    | ✅    | 🔄    |
| 5.5      |      |      |      |      | ✅   | ⚠️    | ✅   | ✅   | ✅    | ✅    | 🔄    |
| 5.6      |      |      |      |      |      | ✅   | ✅   | ✅   | ✅    | ✅    | 🔄    |
| 5.7      |      |      |      |      |      |      | ✅   | ✅   | ✅    | ✅    | 🔄    |
| 5.8      |      |      |      |      |      |      |      | ✅   | ✅    | ✅    | 🔄    |
| 5.9      |      |      |      |      |      |      |      |      | ✅    | ✅    | ✅    |
| 5.10?    |      |      |      |      |      |      |      |      |       | ✅    | ✅    |
| 6.0?     |      |      |      |      |      |      |      |      |       |       | ✅    |

- [1] Old limiter configs should be deleted from the config files (`etc/emqx.conf` and `data/configs/cluster-override.conf`) before upgrade.
- [2] Pre-5.4 routing table will be deleted. Upgrade to 5.9 first, then perform a full-cluster restart (not rolling) before upgrade to 5.10 or later.

## License

See [LICENSE](./LICENSE).
