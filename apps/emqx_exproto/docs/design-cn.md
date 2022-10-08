# 多语言 - 协议接入

`emqx-exproto` 插件用于协议解析的多语言支持。它能够允许其他编程语言（例如：Python，Java 等）直接处理数据流实现协议的解析，并提供 Pub/Sub 接口以实现与系统其它组件的通信。

该插件给 EMQX 带来的扩展性十分的强大，它能以你熟悉语言处理任何的私有协议，并享受由 EMQX 系统带来的高连接，和高并发的优点。

## 特性

- 极强的扩展能力。使用 gRPC 作为 RPC 通信框架，支持各个主流编程语言
- 高吞吐。连接层以完全的异步非阻塞式 I/O 的方式实现
- 连接层透明。完全的支持 TCP\TLS UDP\DTLS 类型的连接管理，并对上层提供统一的 API 接口
- 连接层的管理能力。例如，最大连接数，连接和吞吐的速率限制，IP 黑名单 等

## 架构

![Extension-Protocol Arch](images/exproto-arch.jpg)

该插件主要需要处理的内容包括：

1.  **连接层：** 该部分主要 **维持 Socket 的生命周期，和数据的收发**。它的功能要求包括：
    - 监听某个端口。当有新的 TCP/UDP 连接到达后，启动一个连接进程，来维持连接的状态。
    - 调用 `OnSocketCreated` 回调。用于通知外部模块**已新建立了一个连接**。
    - 调用 `OnScoektClosed` 回调。用于通知外部模块连接**已关闭**。
    - 调用 `OnReceivedBytes` 回调。用于通知外部模块**该连接新收到的数据包**。
    - 提供 `Send` 接口。供外部模块调用，**用于发送数据包**。
    - 提供 `Close` 接口。供外部模块调用，**用于主动关闭连接**。

2. **协议/会话层：**该部分主要**提供 PUB/SUB 接口**，以实现与 EMQX Broker 系统的消息互通。包括：

    - 提供 `Authenticate` 接口。供外部模块调用，用于向集群注册客户端。
    - 提供 `StartTimer` 接口。供外部模块调用，用于为该连接进程启动心跳等定时器。
    - 提供 `Publish` 接口。供外部模块调用，用于发布消息 EMQX Broker 中。
    - 提供 `Subscribe` 接口。供外部模块调用，用于订阅某主题，以实现从 EMQX Broker 中接收某些下行消息。
    - 提供 `Unsubscribe` 接口。供外部模块调用，用于取消订阅某主题。
    - 调用 `OnTimerTimeout` 回调。用于处理定时器超时的事件。
    - 调用 `OnReceivedMessages` 回调。用于接收下行消息（在订阅主题成功后，如果主题上有消息，便会回调该方法）

## 接口设计

从 gRPC 上的逻辑来说，emqx-exproto 会作为客户端向用户的 `ConnectionHandler` 服务发送回调请求。同时，它也会作为服务端向用户提供 `ConnectionAdapter` 服务，以提供 emqx-exproto 各个接口的访问。如图：

![Extension Protocol gRPC Arch](images/exproto-grpc-arch.jpg)


详情参见：`priv/protos/exproto.proto`，例如接口的定义有：

```protobuf
syntax = "proto3";

package emqx.exproto.v1;

// The Broker side serivce. It provides a set of APIs to
// handle a protcol access
service ConnectionAdapter {

  // -- socket layer

  rpc Send(SendBytesRequest) returns (CodeResponse) {};

  rpc Close(CloseSocketRequest) returns (CodeResponse) {};

  // -- protocol layer

  rpc Authenticate(AuthenticateRequest) returns (CodeResponse) {};

  rpc StartTimer(TimerRequest) returns (CodeResponse) {};

  // -- pub/sub layer

  rpc Publish(PublishRequest) returns (CodeResponse) {};

  rpc Subscribe(SubscribeRequest) returns (CodeResponse) {};

  rpc Unsubscribe(UnsubscribeRequest) returns (CodeResponse) {};
}

service ConnectionHandler {

  // -- socket layer

  rpc OnSocketCreated(stream SocketCreatedRequest) returns (EmptySuccess) {};

  rpc OnSocketClosed(stream SocketClosedRequest) returns (EmptySuccess) {};

  rpc OnReceivedBytes(stream ReceivedBytesRequest) returns (EmptySuccess) {};

  // -- pub/sub layer

  rpc OnTimerTimeout(stream TimerTimeoutRequest) returns (EmptySuccess) {};

  rpc OnReceivedMessages(stream ReceivedMessagesRequest) returns (EmptySuccess) {};
}
```

## 配置项设计

1. 以 **监听器(Listener)** 为基础，提供 TCP/UDP 的监听。
   - Listener 目前仅支持：TCP、TLS、UDP、DTLS。(ws、wss、quic 暂不支持)
2. 每个监听器，会指定一个 `ConnectionHandler` 的服务地址，用于调用外部模块的接口。
3. emqx-exproto 还会监听一个 gRPC 端口用于提供对 `ConnectionAdapter` 服务的访问。

例如：

``` properties
## gRPC 服务监听地址 (HTTP)
##
exproto.server.http.url = http://127.0.0.1:9002

## gRPC 服务监听地址 (HTTPS)
##
exproto.server.https.url = https://127.0.0.1:9002
exproto.server.https.cacertfile = ca.pem
exproto.server.https.certfile = cert.pem
exproto.server.https.keyfile = key.pem

## Listener 配置
## 例如，名称为 protoname 协议的 TCP 监听器配置
exproto.listener.protoname = tcp://0.0.0.0:7993

## ConnectionHandler 服务地址及 https 的证书配置
exproto.listener.protoname.connection_handler_url = http://127.0.0.1:9001
#exproto.listener.protoname.connection_handler_certfile =
#exproto.listener.protoname.connection_handler_cacertfile =
#exproto.listener.protoname.connection_handler_keyfile =

# ...
```
