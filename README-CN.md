# EMQX Broker

[![GitHub Release](https://img.shields.io/github/release/emqx/emqx?color=brightgreen)](https://github.com/emqx/emqx/releases)
[![Build Status](https://travis-ci.org/emqx/emqx.svg)](https://travis-ci.org/emqx/emqx)
[![Coverage Status](https://coveralls.io/repos/github/emqx/emqx/badge.svg)](https://coveralls.io/github/emqx/emqx)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/emqx)](https://hub.docker.com/r/emqx/emqx)
[![Slack](https://img.shields.io/badge/Slack-EMQ-39AE85?logo=slack)](https://slack-invite.emqx.io/)
[![Twitter](https://img.shields.io/badge/Twitter-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-EMQ%20X-yellow)](https://askemq.com)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ%20中文-FF0000?logo=youtube)](https://www.youtube.com/channel/UCir_r04HIsLjf2qqyZ4A8Cg)

[English](./README.md) | 简体中文 | [日本語](./README-JP.md) | [русский](./README-RU.md)

*EMQX* 是一款完全开源，高度可伸缩，高可用的分布式 MQTT 消息服务器，适用于 IoT、M2M 和移动应用程序，可处理千万级别的并发客户端。

从 3.0 版本开始，*EMQX* 完整支持 MQTT V5.0 协议规范，向下兼容 MQTT V3.1 和 V3.1.1，并支持 MQTT-SN、CoAP、LwM2M、WebSocket 和 STOMP 等通信协议。EMQX 3.0 单集群可支持千万级别的 MQTT 并发连接。

- 新功能的完整列表，请参阅 [EMQX Release Notes](https://github.com/emqx/emqx/releases)。
- 获取更多信息，请访问 [EMQX 官网](https://www.emqx.cn/)。

## 安装

*EMQX* 是跨平台的，支持 Linux、Unix、macOS 以及 Windows。这意味着 *EMQX* 可以部署在 x86_64 架构的服务器上，也可以部署在 Raspberry Pi 这样的 ARM 设备上。

Windows 上编译和运行 *EMQX* 的详情参考：[Windows.md](./Windows.md)

#### EMQX Docker 镜像安装

```
docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx
```

#### 二进制软件包安装

需从 [EMQX 下载](https://www.emqx.cn/downloads) 页面获取相应操作系统的二进制软件包。

- [单节点安装文档](https://docs.emqx.cn/broker/latest/getting-started/install.html)
- [集群配置文档](https://docs.emqx.cn/broker/latest/advanced/cluster.html)

## 从源码构建

3.0 版本开始，构建 *EMQX* 需要 Erlang/OTP R21+。

4.3 及以后的版本：

```bash
git clone https://github.com/emqx/emqx.git
cd emqx
make
_build/emqx/rel/emqx/bin console
```

对于 4.3 之前的版本，通过另外一个仓库构建：

```bash
git clone https://github.com/emqx/emqx-rel.git
cd emqx-rel
make
_build/emqx/rel/emqx/bin/emqx console
```

## 快速入门

如果 emqx 从源码编译，`cd _build/emqx/rel/emqx`。
如果 emqx 通过 zip 包安装，则切换到 emqx 的根目录。

```
# Start emqx
./bin/emqx start

# Check Status
./bin/emqx_ctl status

# Stop emqx
./bin/emqx stop
```

*EMQX* 启动，可以使用浏览器访问 http://localhost:18083 来查看 Dashboard。

## 测试

### 执行所有测试

```
make eunit ct
```

### 执行部分应用的 common tests

```bash
make apps/emqx_bridge_mqtt-ct
```

### 静态分析(Dialyzer)
##### 分析所有应用程序
```
make dialyzer
```

##### 要分析特定的应用程序，（用逗号分隔的应用程序列表）
```
DIALYZER_ANALYSE_APP=emqx_lwm2m,emqx_auth_jwt,emqx_auth_ldap make dialyzer
```

## 社区

### FAQ

访问 [EMQX FAQ](https://docs.emqx.cn/broker/latest/faq/faq.html) 以获取常见问题的帮助。

### 问答

[GitHub Discussions](https://github.com/emqx/emqx/discussions)
[EMQ 中文问答社区](https://askemq.com)

### 参与设计

如果对 EMQX 有改进建议，可以向[EIP](https://github.com/emqx/eip) 提交 PR 和 ISSUE

### 插件开发

如果想集成或开发你自己的插件，参考 [lib-extra/README.md](./lib-extra/README.md)


### 联系我们

你可通过以下途径与 EMQ 社区及开发者联系:

- [Slack](https://slack-invite.emqx.io)
- [Twitter](https://twitter.com/EMQTech)
- [Facebook](https://www.facebook.com/emqxmqtt)
- [Reddit](https://www.reddit.com/r/emqx/)
- [Weibo](https://weibo.com/emqtt)
- [Blog](https://www.emqx.cn/blog)

欢迎你将任何 bug、问题和功能请求提交到 [emqx/emqx](https://github.com/emqx/emqx/issues)。

## MQTT 规范

你可以通过以下链接了解与查阅 MQTT 协议:

[MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)

[MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html)

[MQTT SN](http://mqtt.org/new/wp-content/uploads/2009/06/MQTT-SN_spec_v1.2.pdf)

## 开源许可

Apache License 2.0, 详见 [LICENSE](./LICENSE)。
