[English](./README.md) | 简体中文 | [Русский](./README-RU.md)

# EMQX

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen&label=Release)](https://github.com/emqx/emqx/releases)
[![Build Status](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml/badge.svg)](https://github.com/emqx/emqx/actions/workflows/_push-entrypoint.yaml)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Discord](https://img.shields.io/discord/931086341838622751?label=Discord&logo=discord)](https://discord.gg/xYGf3fQnES)
[![X](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=x)](https://x.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-EMQX-yellow)](https://askemq.com)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ%20中文-FF0000?logo=youtube)](https://www.youtube.com/channel/UCir_r04HIsLjf2qqyZ4A8Cg)

EMQX 是全球最具扩展性和可靠性的 MQTT 平台，专为高性能、高可靠、高安全的物联网数据基础设施而设计。它支持 MQTT 5.0、3.1.1 和 3.1，以及 MQTT-SN、CoAP、LwM2M 和 MQTT over QUIC 等其他协议。EMQX 允许您连接数百万物联网设备，实时处理和路由消息，并与广泛的后端数据系统集成。它非常适合人工智能、物联网、工业物联网 (IIoT)、车联网、智慧城市等应用。

**自 v5.9.0 起，EMQX 已将先前开源版和企业版的所有功能统一到一个采用 Business Source License (BSL) 1.1 的强大产品中。**

如果您想了解我们为何做出此更改，请阅读此[博客文章](https://www.emqx.com/zh/news/emqx-adopts-business-source-license)。

有关 BSL 1.1 的更多详细信息，请参阅[许可证](#License)部分。

## 核心功能

EMQX 为现代连接系统提供了一套强大的功能：

### 全面的协议支持

- 完全支持 MQTT v5.0、v3.1.1 和 v3.1。
- MQTT over QUIC：利用 QUIC 的优势实现更快的连接建立、减少队头阻塞以及无缝的连接迁移。
- 通过网关支持 LwM2M、CoAP、MQTT-SN 等其他物联网协议。

### 海量扩展与高可用性

- 单集群支持连接超过 1 亿的并发 MQTT 客户端。
- 以亚毫秒级延迟每秒处理数百万条消息。
- 无主集群实现高可用性和容错能力。

### 强大的规则引擎与数据集成

- 基于 SQL 的规则引擎，用于处理、转换、丰富和过滤动态数据。
- 与 50 多种云服务和企业系统无缝桥接和集成数据，包括：
    - **消息队列**：Kafka、RabbitMQ、Pulsar、RocketMQ 等。
    - **数据库**：PostgreSQL、MySQL、MongoDB、Redis、ClickHouse、InfluxDB 等。
    - **云服务**：AWS Kinesis、GCP Pub/Sub、Azure Event Hubs、Confluent Cloud 等。
- Webhook 支持，方便与自定义服务集成。

### 可靠的安全保障

- 使用 TLS/SSL 和 WSS 的安全连接。
- 灵活的身份验证机制：用户名/密码、JWT、PSK、X.509 证书等。
- 使用 ACL 进行精细的访问控制。
- 与外部身份验证数据库（LDAP、SQL、NoSQL）集成。

### 先进的可观察性与管理

- 通过 Prometheus、Grafana 和 OpenTelemetry 进行全面监控。
- 详细的日志记录和追踪功能。
- 用户友好的 Dashboard，用于集群概览和管理。
- 丰富的 HTTP API，用于自动化和第三方集成。

### 可扩展性

- 用于扩展功能的插件架构。
- 用于在消息生命周期各个点自定义行为的钩子 (Hooks)。

### 统一体验

- 采用 BSL 1.1 许可证（从 v5.9.0 开始），所有功能（包括以前企业版独有的功能）均对所有开发者可用。

## 快速开始

### 试用 EMQX Cloud

设置 EMQX 最简单的方法是使用 EMQX Cloud 创建一个全托管的部署。您可以[免费试用 EMQX Cloud](https://www.emqx.com/zh/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud.emqx.com/console/deployments/0?oper=new)。

- [EMQX Serverless](https://www.emqx.com/zh/cloud/serverless-mqtt)
- [EMQX Dedicated](https://www.emqx.com/zh/cloud/dedicated)
- [EMQX BYOC](https://www.emqx.com/zh/cloud/byoc)

### 使用 Docker 运行单节点

```bash
docker run -d --name emqx \
  -p 1883:1883 -p 8083:8083 -p 8084:8084 \
  -p 8883:8883 -p 18083:18083 \
  emqx/emqx-enterprise:latest
```

接下来，请遵循 [使用 Docker 安装 EMQX](https://docs.emqx.com/zh/emqx/latest/deploy/install-docker.html) 指南获取进一步说明。

### 在 Kubernetes 上运行 EMQX 集群

请参考官方 [EMQX Operator 文档](https://docs.emqx.com/zh/emqx-operator/latest/getting-started/getting-started.html) 获取详细信息。

### 下载 EMQX

如果您倾向于自行安装和管理 EMQX，可以从[官网](https://www.emqx.com/zh/downloads-and-install/enterprise)下载最新版本。

更多安装选项，请参阅 [EMQX 安装文档](https://docs.emqx.com/zh/emqx/latest/deploy/install.html)。

## 文档

EMQX (本地部署)：[docs.emqx.com/zh/emqx/latest/](https://docs.emqx.com/zh/emqx/latest/)。
EMQX Cloud：[docs.emqx.com/zh/cloud/latest/](https://docs.emqx.com/zh/cloud/latest/)。

## 贡献

请参阅我们的[贡献指南](./CONTRIBUTING.md)。

对于更系统的改进建议，您可以向 [EIP](https://github.com/emqx/eip) 提交拉取请求 (Pull Request)。

## 社区

- 访问 [EMQ 问答社区](https://askemq.com/) 以获取帮助，也可以分享您的想法或项目。
- 报告 Bug：[GitHub Issues](https://github.com/emqx/emqx/issues)。
- Discord：[EMQX Discord 服务器](https://discord.gg/xYGf3fQnES)。
- 添加小助手微信号 `emqmkt`，加入 EMQ 微信技术交流群。
- 关注我们的 [Bilibili](https://space.bilibili.com/522222081)，获取最新物联网技术分享。
- 关注我们的 [微博](https://weibo.com/emqtt) 或 [X (原 Twitter)](https://x.com/EMQTech)，获取 EMQ 最新资讯。
- 订阅我们的 [YouTube 频道](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q) (英文) 或 [EMQ 中文 YouTube 频道](https://www.youtube.com/channel/UCir_r04HIsLjf2qqyZ4A8Cg)。

## 相关资源

- EMQX 官网：[emqx.com/zh](https://www.emqx.com/zh)
- EMQX 博客：[emqx.com/zh/blog](https://www.emqx.com/zh/blog)
- MQTT 客户端编程：[教程](https://www.emqx.com/zh/blog/category/mqtt-programming)
- MQTT SDK：[热门 SDK](https://www.emqx.com/zh/mqtt-client-sdk)
- MQTT 工具：[MQTTX](https://mqttx.app/zh)
- [车联网平台搭建从入门到精通](https://www.emqx.com/zh/blog/category/internet-of-vehicles)

## 从源码构建

master 分支追踪最新的版本 5。

- EMQX 5.4 及更新版本可以使用 OTP 25 或 26 构建。
- EMQX 5.9+ 可以使用 OTP 27 构建。

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx-enterprise/rel/emqx/bin/emqx console
```

对于 4.2 或更早的版本，release 版本必须从另一个仓库构建。

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## 5.0 版本以来的滚动升级路径

下表是自 5.0 版本以来支持的滚动升级路径。

- 以 `?` 结尾的版本号（例如 `6.0?`）是未来的版本。
- ✅: 支持，或计划支持。
- ⚠️:  可能会遇到问题，需要手动解决。
- ❌: 不支持。
- 🔄: 未来版本的初步支持。

详细信息请参阅版本说明。

| 从\到    | 5.1  | 5.2  | 5.3  | 5.4  | 5.5  | 5.6  | 5.7  | 5.8  | 5.9   | 5.10? | 6.0?  |
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

- [1] 升级前应从配置文件（`etc/emqx.conf` 和 `data/configs/cluster-override.conf`）中删除旧的限流器配置。
- [2] 5.4 版本之前的路由表将被删除。请先升级到 5.9 版本，然后在升级到 5.10 或更高版本之前执行一次全集群重启（非滚动重启）。

## 许可证

### 重要许可证更新

自 **5.9.0** 版本起，EMQX 已从 Apache 2.0 迁移到 Business Source License (BSL) 1.1。

### 集群部署的许可证要求 (v5.9.0+)

从 EMQX v5.9.0 开始，由于许可证变更和所有功能的统一，部署 EMQX 集群（超过 1 个节点）需要加载许可证文件。

有关许可证获取、申请以及 BSL 1.1 的具体细节，请参阅以下资源：

- **新闻**：[EMQX 采用 Business Source License](https://www.emqx.com/zh/news/emqx-adopts-business-source-license)
- **博客**：[采用 Business Source License 加速 MQTT 和人工智能创新](https://www.emqx.com/zh/blog/adopting-business-source-license-to-accelerate-mqtt-and-ai-innovation)
- **常见问题解答**：[EMQX 许可证常见问题解答](https://www.emqx.com/zh/content/license-faq)
