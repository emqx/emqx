# 多语言 - 协议接入

`emqx-exproto` 插件用于协议解析的多语言支持。它能够允许其他编程语言（例如：Python，Java 等）直接处理数据流实现协议的解析，并提供 Pub/Sub 接口以实现与系统其它组件的通信。

该插件给 EMQ X 带来的扩展性十分的强大，它能以你熟悉语言处理任何的私有协议，并享受由 EMQ X 系统带来的高连接，和高并发的优点。

**声明：当前仅实现了 Python、Java 的支持**

## 特性

- 多语言支持。快速将接入层的协议实现迁移到 EMQ X 中进行管理
- 高吞吐。连接层以完全的异步非阻塞式 I/O 的方式实现
- 完善的连接层。完全的支持 TCP\TLS UDP\DTLS 类型的连接
- 连接层的管理能力。例如，最大连接数，连接和吞吐的速率限制，IP 黑名单 等

##  架构

![Extension-Protocol Arch](images/exproto-arch.jpg)

该插件需要完成的工作包括三部分：

**初始化：** (TODO)
- loaded:
- unload:

**连接层：** 该部分主要**维持 Socket 的生命周期，和数据的收发**。它的功能要求包括：

- 监听某个端口。当有新的 TCP/UDP 连接到达后，启动一个连接进程，来维持连接的状态。
- 调用 `init` 回调。用于通知外部模块**已新建立了一个连接**。
- 调用 `terminated` 回调。用于通知外部模块连接**已关闭**。
- 调用 `received` 回调。用于通知外部模块**该连接新收到的数据包**。
- 提供 `send` 接口。供外部模块调用，**用于发送数据包**。
- 提供 `close` 接口。供外部模块调用，**用于主动关闭连接**。


**协议/会话层：**该部分主要**提供 PUB/SUB 接口**，以实现与 EMQ X Broker 系统的消息互通。包括：

- 提供 `register` 接口。供外部模块调用，用于向集群注册客户端。
- 提供 `publish` 接口。供外部模块调用，用于发布消息 EMQ X Broker 中。
- 提供 `subscribe` 接口。供外部模块调用，用于订阅某主题，以实现从 EMQ X Broker 中接收某些下行消息。
- 提供 `unsubscribe` 接口。供外部模块调用，用于取消订阅某主题。
- 调用 `deliver` 回调。用于接收下行消息（在订阅主题成功后，如果主题上有消息，便会回调该方法）


**管理&统计相关：** 该部分主要提供其他**管理&统计相关的接口**。包括：

- 提供 `Hooks` 类的接口。用于与系统的钩子系统进行交互。
- 提供 `Metrics` 类的接口。用于统计。
- 提供 `HTTP or CLI` 管理类接口。

## 接口设计

### 连接层接口

多语言组件需要向 EMQ X 注册的回调函数：

```erlang
%% Got a new Connection
init(conn(), conninfo()) -> state().

%% Incoming a data
recevied(conn(), data(), state()) -> state().

%% Socket & Connection process terminated
terminated(conn(), reason(), state()) -> ok.

-opaue conn() :: pid().

-type conninfo() :: [ {socktype, tcp | tls | udp | dtls},
                    , {peername, {inet:ip_address(), inet:port_number()}},
                    , {sockname, {inet:ip_address(), inet:port_number()}},
                    , {peercert, nossl | [{cn, string()}, {dn, string()}]}
                    ]).

-type reason() :: string().

-type state() :: any().
```


`emqx-exproto` 需要向多语言插件提供的接口：

``` erlang
%% Send a data to socket
send(conn(), data()) -> ok.

%% Close the socket
close(conn() ) -> ok.
```


### 协议/会话层接口

多语言组件需要向 EMQ X 注册的回调函数：

```erlang
%% Received a message from a Topic
deliver(conn(), [message()], state()) -> state().

-type message() :: [ {id, binary()}
                   , {qos, integer()}
                   , {from, binary()}
                   , {topic, binary()}
                   , {payload, binary()}
                   , {timestamp, integer()}
                   ].
```


`emqx-exproto` 需要向多语言插件提供的接口：

``` erlang
%% Reigster the client to Broker
register(conn(), clientinfo()) -> ok | {error, Reason}.

%% Publish a message to Broker
publish(conn(), message()) -> ok.

%% Subscribe a topic
subscribe(conn(), topic(), qos()) -> ok.

%% Unsubscribe a topic
unsubscribe(conn(), topic()) -> ok.

-type clientinfo() :: [ {proto_name, binary()}
                      , {proto_ver, integer() | string()}
                      , {clientid, binary()}
                      , {username, binary()}
                      , {mountpoint, binary()}}
                      , {keepalive, non_neg_integer()}
                      ].
```

### 管理&统计相关接口

*TODO..*

## 配置项设计

1. 以 **监听器( Listener)** 为基础，提供 TCP/UDP 的监听。
   - Listener 目前仅支持：TCP、TLS、UDP、DTLS。(ws、wss、quic 暂不支持)
2. 每个监听器，会指定一个多语言的驱动，用于调用外部模块的接口
   - Driver 目前仅支持：python，java

例如：

``` properties
## A JT/T 808 TCP based example:
exproto.listener.jtt808 = 6799
exproto.listener.jtt808.type = tcp
exproto.listener.jtt808.driver = python
# acceptors, max_connections, max_conn_rate, ...
# proxy_protocol, ...
# sndbuff, recbuff, ...
# ssl, cipher, certfile, psk, ...

exproto.listener.jtt808.<key> = <value>

## A CoAP UDP based example
exproto.listener.coap = 6799
exproto.listener.coap.type = udp
exproto.listener.coap.driver = java
# ...
```

## 集成与调试

参见 SDK 规范、和对应语言的开发手册

## SDK 实现要求

参见 SDK 规范、和对应语言的开发手册

## TODOs:

- 认证 和 发布 订阅鉴权等钩子接入
