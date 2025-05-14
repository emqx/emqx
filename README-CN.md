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

有关 BSL 1.1 的更多详细信息，请参阅[许可证](#许可证)部分。

## 核心功能

EMQX 为新一代物联网系统提供了一系列强大的功能：

### 全面的协议支持
- 完全支持 MQTT v5.0、v3.1.1 和 v3.1。
- [MQTT over QUIC](https://docs.emqx.com/zh/emqx/latest/mqtt-over-quic/introduction.html)：利用 QUIC 的优势实现更快的连接建立、减少队头阻塞以及无缝的连接迁移。
- 通过[网关](https://docs.emqx.com/zh/emqx/latest/gateway/gateway.html)支持 [LwM2M](https://docs.emqx.com/zh/emqx/latest/gateway/lwm2m.html)、[CoAP](https://docs.emqx.com/zh/emqx/latest/gateway/coap.html)、[MQTT-SN](https://docs.emqx.com/zh/emqx/latest/gateway/mqttsn.html) 等其他物联网协议。

### 海量扩展与高可用性
- 单集群支持[连接](https://www.emqx.com/zh/solutions/iot-device-connectivity)超过 1 亿的并发 MQTT 客户端。
- 以亚毫秒级延迟每秒[处理](https://www.emqx.com/zh/solutions/reliable-mqtt-messaging)数百万条消息。
- [无主集群](https://docs.emqx.com/zh/emqx/latest/deploy/cluster/introduction.html)实现高可用性和容错能力。
- 通过 [EMQX Cluster Linking](https://www.emqx.com/zh/solutions/cluster-linking) 实现无缝的全球通信。

### 强大的规则引擎与数据集成
- 基于 SQL 的[规则引擎](https://www.emqx.com/zh/solutions/mqtt-data-processing)用于处理、转换、丰富和过滤动态数据。
- 与 50 多种云服务和企业系统无缝桥接和[集成](https://www.emqx.com/zh/solutions/mqtt-data-integration)数据，包括：
    - **消息队列**：[Kafka](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-kafka.html)、[RabbitMQ](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-rabbitmq.html)、[Pulsar](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-pulsar.html)、[RocketMQ](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-rocketmq.html) 等。
    - **数据库**：[PostgreSQL](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-pgsql.html)、[MySQL](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-mysql.html)、[MongoDB](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-mongodb.html)、[Redis](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-redis.html)、[ClickHouse](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-clickhouse.html)、[InfluxDB](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-influxdb.html) 等。
    - **云服务**：[AWS Kinesis](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-kinesis.html)、[GCP Pub/Sub](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-gcp-pubsub.html)、[Azure Event Hubs](https://docs.emqx.com/zh/emqx/latest/data-integration/data-bridge-azure-event-hub.html)、[Confluent Cloud](https://docs.emqx.com/zh/emqx/latest/data-integration/confluent-sink.html) 等。
- [Webhook](https://docs.emqx.com/zh/emqx/latest/data-integration/webhook.html) 支持，方便与自定义服务集成。

### [Flow Designer](https://docs.emqx.com/zh/emqx/latest/flow-designer/introduction.html)
- 拖放式画布，使用节点进行规则、集成和 AI 任务，无需编写代码即可编排实时数据流。

### [Smart Data Hub](https://docs.emqx.com/zh/cloud/latest/data_hub/smart_data_hub.html)
- [Schema Registry (模式注册表)](https://docs.emqx.com/zh/cloud/latest/data_hub/schema_registry.html)：定义、存储和管理数据模式以确保一致性。
- [Schema Validation (模式验证)](https://docs.emqx.com/zh/cloud/latest/data_hub/schema_validation.html)：根据注册的模式验证传入数据以维护数据完整性。
- [Message Transformation (消息转换)](https://docs.emqx.com/zh/cloud/latest/data_hub/message_transformation.html)：在不同格式和结构之间转换数据以促进无缝集成。

### [AI 处理与集成](https://www.emqx.com/zh/solutions/artificial-intelligence):
- 针对物联网数据流的原生 AI 处理能力。
- 与流行的 AI 服务集成。
- 支持在边缘或云端进行 AI 驱动的决策。

### 可靠的[安全保障](https://www.emqx.com/zh/solutions/mqtt-security)
- 使用 TLS/SSL 和 WSS 的[安全连接](https://docs.emqx.com/zh/emqx/latest/network/overview.html)。
- 灵活的[身份验证](https://docs.emqx.com/zh/emqx/latest/access-control/authn/authn.html)机制：用户名/密码、JWT、PSK、X.509 证书等。
- 使用 [ACL](https://docs.emqx.com/zh/emqx/latest/access-control/authz/authz.html) 进行精细的访问控制。
- 与外部身份验证数据库（[LDAP](https://docs.emqx.com/zh/emqx/latest/access-control/authn/ldap.html)、[SQL](https://docs.emqx.com/zh/emqx/latest/access-control/authn/postgresql.html)、[Redis](https://docs.emqx.com/zh/emqx/latest/access-control/authn/redis.html)）集成。

### 先进的可观察性与管理
- 通过 [Prometheus](https://docs.emqx.com/zh/emqx/latest/observability/prometheus.html)、[Grafana](https://grafana.com/grafana/dashboards/17446-emqx/)、[Datadog](https://docs.emqx.com/zh/emqx/latest/observability/datadog.html) 和 [OpenTelemetry](https://docs.emqx.com/zh/emqx/latest/observability/opentelemetry/opentelemetry.html) 进行全面监控。
- 详细的日志记录和[追踪](https://docs.emqx.com/zh/emqx/latest/observability/tracer.html)功能。
- 用户友好的 [Dashboard](https://docs.emqx.com/zh/emqx/latest/dashboard/introduction.html)，用于集群概览和管理。
- 丰富的 [HTTP API](https://docs.emqx.com/zh/emqx/latest/admin/api.html)，用于自动化和第三方集成。

### 可扩展性
- 用于扩展功能的[插件](https://docs.emqx.com/zh/emqx/latest/extensions/plugins.html)架构。
- 用于在消息生命周期各个点自定义行为的[钩子 (Hooks)](https://docs.emqx.com/zh/emqx/latest/extensions/hooks.html)。

### 统一体验
- 采用 BSL 1.1 许可证（从 v5.9.0 开始），所有功能（包括以前企业版独有的功能）均对所有开发者可用。

## 快速开始

### 试用 EMQX Cloud

使用 EMQX 最简单的方式是在 EMQX Cloud 创建一个全托管的部署。您可以[免费试用 EMQX Cloud](https://www.emqx.com/zh/signup?utm_source=github.com&utm_medium=referral&utm_campaign=emqx-readme-to-cloud&continue=https://cloud.emqx.com/console/deployments/0?oper=new)，无需绑定信用卡。

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

- EMQX (本地部署)：[docs.emqx.com/zh/emqx/latest/](https://docs.emqx.com/zh/emqx/latest/)。
- EMQX Cloud：[docs.emqx.com/zh/cloud/latest/](https://docs.emqx.com/zh/cloud/latest/)。

## 贡献

请参阅我们的[贡献指南](./CONTRIBUTING.md)。

对于更系统的改进建议，您可以向 [EIP](https://github.com/emqx/eip) 提交拉取请求 (Pull Request)。

## 社区

- 访问 [EMQ 问答社区](https://askemq.com/) 以获取帮助，也可以分享您的想法或项目。
- 提问：[GitHub Discussions](https://github.com/emqx/emqx/discussions) 或 [EMQX Community Slack](https://slack-invite.emqx.io/)。
- 添加小助手微信号 `emqmkt`，加入 EMQ 微信技术交流群。
- 加入我们的 [Discord](https://discord.gg/xYGf3fQnES)，参于实时讨论。
- 关注我们的 [Bilibili](https://space.bilibili.com/522222081)，获取最新物联网技术分享。
- 报告 Bug：[GitHub Issues](https://github.com/emqx/emqx/issues)。
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

- [1] 升级前应从配置文件（`etc/emqx.conf` 和 `data/configs/cluster-override.conf`）中删除旧的 limiter 配置。
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
