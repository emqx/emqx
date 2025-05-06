简体中文 | [English](./README.md) | [Русский](./README-RU.md)

# EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml/badge.svg)](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx?label=Docker%20Pulls)](https://hub.docker.com/r/emqx/emqx)
[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/emqx/emqx?label=OpenSSF%20Scorecard&style=flat)](https://securityscorecards.dev/viewer/?uri=github.com/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![X](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=x)](https://x.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-EMQX-yellow)](https://askemq.com)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ%20中文-FF0000?logo=youtube)](https://www.youtube.com/channel/UCir_r04HIsLjf2qqyZ4A8Cg)


EMQX 是一款全球下载量超千万的大规模分布式物联网 MQTT 服务器，单集群支持 1 亿物联网设备连接，消息分发时延低于 1 毫秒。为高可靠、高性能的物联网实时数据移动、处理和集成提供动力，助力企业构建关键业务的 IoT 平台与应用。

EMQX 自 2013 年在 GitHub 发布开源版本以来，获得了来自 60 多个国家和地区的 20000 余家企业用户的广泛认可，连接全球超过 2.5 亿台智能联网设备。

更多信息请访问 [EMQX 官网](https://www.emqx.com/zh)。

## 快速开始

#### EMQX Cloud

使用 EMQX 最简单的方式是在 EMQX Cloud 上创建完全托管的 MQTT 服务。[免费试用 EMQX Cloud](https://www.emqx.com/zh/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud.emqx.com/console/deployments/0?oper=new)，无需绑定信用卡。

#### 使用 Docker 运行 EMQX

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 emqx/emqx-enterprise:latest
```

接下来请参考 [入门指南](https://docs.emqx.com/zh/emqx/latest/deploy/install-docker.html) 开启您的 EMQX 之旅。

#### 在 Kubernetes 上运行 EMQX 集群

请参考 [EMQX Operator 文档](https://docs.emqx.com/zh/emqx-operator/latest/getting-started/getting-started.html)。

#### 更多安装方式

您可以从 [emqx.com/zh/downloads-and-install/broker](https://www.emqx.com/zh/downloads-and-install/enterprise) 下载不同格式的 EMQX 安装包进行手动安装。

也可以直接访问 [EMQX 安装文档](https://docs.emqx.com/zh/emqx/latest/deploy/install.html) 查看不同安装方式的操作步骤。

## 文档

EMQX 开源版文档：[docs.emqx.com/zh/emqx/latest](https://docs.emqx.com/zh/emqx/latest/)。

EMQX Cloud 文档：[docs.emqx.com/zh/cloud/latest](https://docs.emqx.com/zh/cloud/latest/)。

## 贡献

请参考我们的 [贡献者指南](./CONTRIBUTING.md)。

如果对 EMQX 有改进建议，可以向 [EIP](https://github.com/emqx/eip) 提交 PR 和 ISSUE。

## 社区

- 访问 [EMQ 问答社区](https://askemq.com/) 以获取帮助，也可以分享您的想法或项目。
- 添加小助手微信号 `emqmkt`，加入 EMQ 微信技术交流群。
- 加入我们的 [Discord](https://discord.gg/xYGf3fQnES)，参于实时讨论。
- 关注我们的 [Bilibili](https://space.bilibili.com/522222081)，获取最新物联网技术分享。
- 关注我们的 [微博](https://weibo.com/emqtt) 或 [Twitter](https://twitter.com/EMQTech)，获取 EMQ 最新资讯。

## 相关资源

- [MQTT 入门及进阶](https://www.emqx.com/zh/mqtt)

  EMQ 提供了通俗易懂的技术文章及简单易用的客户端工具，帮助您学习 MQTT 并快速入门 MQTT 客户端编程。

- [MQTT SDKs](https://www.emqx.com/zh/mqtt-client-sdk)

  我们选取了各个编程语言中热门的 MQTT 客户端 SDK，并提供代码示例，帮助您快速掌握 MQTT 客户端库的使用。

- [MQTTX](https://mqttx.app/zh)

  优雅的跨平台 MQTT 5.0 客户端工具，提供了桌面端、命令行、Web 三种版本，帮助您更快的开发和调试 MQTT 服务和应用。

- [车联网平台搭建从入门到精通](https://www.emqx.com/zh/blog/category/internet-of-vehicles)

  结合 EMQ 在车联网领域的实践经验，从协议选择等理论知识，到平台架构设计等实战操作，分享如何搭建一个可靠、高效、符合行业场景需求的车联网平台。

## 从源码构建

`master` 分支是最新的 5 版本，`main-v4.4` 是 4.4 版本。

EMQX 4.4 版本需要 OTP 24；5 版本则可以使用 OTP 25 和 26 构建。

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin/emqx console
```

对于 4.2 或更早的版本，需要从另一个仓库构建。

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## 源码许可

详见 [LICENSE](./LICENSE)。
