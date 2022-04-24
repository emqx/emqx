# 设计

## 动机

在 EMQX v4.1-v4.2 中，我们发布了 2 个插件来扩展 emqx 的编程能力：

1. `emqx-extension-hook` 提供了使用 Java, Python 向 Broker 挂载钩子的功能
2. `emqx-exproto` 提供了使用 Java，Python 编写用户自定义协议接入插件的功能

但在后续的支持中发现许多难以处理的问题：

1. 有大量的编程语言需要支持，需要编写和维护如 Go, JavaScript, Lua.. 等语言的驱动。
2. `erlport` 使用的操作系统的管道进行通信，这让用户代码只能部署在和 emqx 同一个操作系统上。部署方式受到了极大的限制。
3. 用户程序的启动参数直接打包到 Broker 中，导致用户开发无法实时的进行调试，单步跟踪等。
4. `erlport` 会占用 `stdin` `stdout`。

因此，我们计划重构这部分的实现，其中主要的内容是：
1. 使用 `gRPC` 替换 `erlport`。
2. 将 `emqx-extension-hook` 重命名为 `emqx-exhook`


旧版本的设计：[emqx-extension-hook design in v4.2.0](https://github.com/emqx/emqx-exhook/blob/v4.2.0/docs/design.md)

## 设计

架构如下：

```
  EMQX
+========================+                 +========+==========+
|    ExHook              |                 |        |          |
|   +----------------+   |      gRPC       | gRPC   |  User's  |
|   |   gRPC Client  | ------------------> | Server |  Codes   |
|   +----------------+   |    (HTTP/2)     |        |          |
|                        |                 |        |          |
+========================+                 +========+==========+
```

`emqx-exhook` 通过 gRPC 的方式向用户部署的 gRPC 服务发送钩子的请求，并处理其返回的值。


和 emqx 原生的钩子一致，emqx-exhook 也按照链式的方式执行：

<img src="https://docs.emqx.net/broker/latest/cn/advanced/assets/chain_of_responsiblity.png" style="zoom:50%;" />

### gRPC 服务示例

用户需要实现的方法，和数据类型的定义在 `priv/protos/exhook.proto` 文件中：

```protobuff
syntax = "proto3";

package emqx.exhook.v2;

service HookProvider {

  rpc OnProviderLoaded(ProviderLoadedRequest) returns (LoadedResponse) {};

  rpc OnProviderUnloaded(ProviderUnloadedRequest) returns (EmptySuccess) {};

  rpc OnClientConnect(ClientConnectRequest) returns (EmptySuccess) {};

  rpc OnClientConnack(ClientConnackRequest) returns (EmptySuccess) {};

  rpc OnClientConnected(ClientConnectedRequest) returns (EmptySuccess) {};

  rpc OnClientDisconnected(ClientDisconnectedRequest) returns (EmptySuccess) {};

  rpc OnClientAuthenticate(ClientAuthenticateRequest) returns (ValuedResponse) {};

  rpc OnClientAuthorize(ClientAuthorizeRequest) returns (ValuedResponse) {};

  rpc OnClientSubscribe(ClientSubscribeRequest) returns (EmptySuccess) {};

  rpc OnClientUnsubscribe(ClientUnsubscribeRequest) returns (EmptySuccess) {};

  rpc OnSessionCreated(SessionCreatedRequest) returns (EmptySuccess) {};

  rpc OnSessionSubscribed(SessionSubscribedRequest) returns (EmptySuccess) {};

  rpc OnSessionUnsubscribed(SessionUnsubscribedRequest) returns (EmptySuccess) {};

  rpc OnSessionResumed(SessionResumedRequest) returns (EmptySuccess) {};

  rpc OnSessionDiscarded(SessionDiscardedRequest) returns (EmptySuccess) {};

  rpc OnSessionTakenover(SessionTakenoverRequest) returns (EmptySuccess) {};

  rpc OnSessionTerminated(SessionTerminatedRequest) returns (EmptySuccess) {};

  rpc OnMessagePublish(MessagePublishRequest) returns (ValuedResponse) {};

  rpc OnMessageDelivered(MessageDeliveredRequest) returns (EmptySuccess) {};

  rpc OnMessageDropped(MessageDroppedRequest) returns (EmptySuccess) {};

  rpc OnMessageAcked(MessageAckedRequest) returns (EmptySuccess) {};
}
```

### 配置文件示例

```
exhook: {
    ## 配置 gRPC 服务地址 (HTTP)
    ##
    ## default 为服务器的名称
    server.default: {
        url: "http://127.0.0.1:9000"
    }
}
```
