# EMQ X Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg)](https://coveralls.io/github/emqx/emqx)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack Invite](<https://slack-invite.emqx.io/badge.svg>)](https://slack-invite.emqx.io)
[![Twitter](https://img.shields.io/badge/Twitter-EMQ%20X-1DA1F2?logo=twitter)](https://twitter.com/emqtt)

[![最棒的物联网 MQTT 开源团队期待您的加入](https://www.emqx.io/static/img/github_readme_cn_bg.png)](https://www.emqx.io/cn/careers)

[English](./README.md) | 简体中文 | [日本語](./README-JP.md)

*EMQ X* 是一款完全开源，高度可伸缩，高可用的分布式 MQTT 消息服务器，适用于 IoT、M2M 和移动应用程序，可处理千万级别的并发客户端。

从 3.0 版本开始，*EMQ X* 完整支持 MQTT V5.0 协议规范，向下兼容 MQTT V3.1 和 V3.1.1，并支持 MQTT-SN、CoAP、LwM2M、WebSocket 和 STOMP 等通信协议。EMQ X 3.0 单集群可支持千万级别的 MQTT 并发连接。

- 新功能的完整列表，请参阅 [EMQ X Release Notes](https://github.com/emqx/emqx/releases)。
- 获取更多信息，请访问 [EMQ X 官网](https://www.emqx.io/cn/)。

## 安装

*EMQ X* 是跨平台的，支持 Linux、Unix、macOS 以及 Windows。这意味着 *EMQ X* 可以部署在 x86_64 架构的服务器上，也可以部署在 Raspberry Pi 这样的 ARM 设备上。

#### EMQ X Docker 镜像安装

```
docker run -d --name emqx -p 1883:1883 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### 二进制软件包安装

需从 [EMQ X 下载](https://www.emqx.io/cn/downloads) 页面获取相应操作系统的二进制软件包。

- [单节点安装文档](https://docs.emqx.io/broker/latest/cn/getting-started/install.html)
- [集群配置文档](https://docs.emqx.io/broker/latest/cn/advanced/cluster.html)

## 从源码构建

3.0 版本开始，构建 *EMQ X* 需要 Erlang/OTP R21+。

```
git clone https://github.com/emqx/emqx-rel.git

cd emqx-rel && make

cd _rel/emqx && ./bin/emqx console
```

## 快速入门

```
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

*EMQ X* 启动，可以使用浏览器访问 http://localhost:18083 来查看 Dashboard。

### 静态分析(Dialyzer)
##### 分析所有应用程序
```
make dialyzer
```

##### 要分析特定的应用程序，（用逗号分隔的应用程序列表）
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_auth_jwt,emqx_auth_ldap make dialyzer
```

## FAQ

访问 [EMQ X FAQ](https://docs.emqx.io/broker/latest/cn/faq/faq.html) 以获取常见问题的帮助。

## 产品路线

通过 [EMQ X Roadmap uses Github milestones](https://github.com/emqx/emqx/milestones) 参与跟踪项目进度。

## 社区、讨论、贡献和支持

你可通过以下途径与 EMQ 社区及开发者联系:

- [Slack](https://slack-invite.emqx.io)
- [Twitter](https://twitter.com/emqtt)
- [Facebook](https://www.facebook.com/emqxmqtt)
- [Reddit](https://www.reddit.com/r/emqx/)
- [Forum](https://groups.google.com/d/forum/emqtt)
- [Weibo](https://weibo.com/emqtt)
- [Blog](https://www.emqx.io/cn/blog)

欢迎你将任何 bug、问题和功能请求提交到 [emqx/emqx](https://github.com/emqx/emqx/issues)。

## MQTT 规范

你可以通过以下链接了解与查阅 MQTT 协议:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## 开源许可

Apache License 2.0, 详见 [LICENSE](./LICENSE)。
