
.. _design:

============
Design Guide
============

.. _design_intro:

----------------
About emqttd 1.0
----------------

Finally, we decide to tag emqttd 1.0 after two years development. An experimental project that try to introcude MQTT protocol to SCADA system.


emqttd消息服务器1.0版本经过两年时间开发，开发方式有点像摇滚乐专辑的制作，最初从0.1版本一些即兴创作的部分开始，但最终在各层架构设计上做出了正确的选择，整体上体现了某种程度的正交(Orthogonality)和一致性(Consistency)。emqttd 1.0可能是目前开源领域唯一一个，几乎不需要用户做太多努力就可以支持到100万连接的MQTT服务器。

C1000K Problem
--------------

多核服务器和现代操作系统内核层面，可以很轻松支持100万TCP连接，核心问题是应用层面如何处理业务瓶颈。

emqttd消息服务器在业务和应用层面，解决了承载100万连接的各类瓶颈问题。连接测试的操作系统内核、TCP协议栈、Erlang虚拟机参数: http://docs.emqtt.cn/zh_CN/latest/tune.html

Fully Asynchronous 
------------------

emqttd消息服务器是基于Erlang/OTP平台的全异步的架构：异步TCP连接处理、异步主题(Topic)订阅、异步消息发布。只有在资源负载限制部分采用同步设计，比如TCP连接创建和Mnesia数据库事务执行。

一条MQTT消息从发布者(Publisher)到订阅者(Subscriber)，在emqttd消息服务器内部异步流过一系列Erlang进程Mailbox::

                      ----------          -----------          ----------
    Publisher --Msg-->| Client | --Msg--> | Session | --Msg--> | Client | --Msg--> Subscriber
                      ----------          -----------          ----------

Message Persistence?
--------------------

emqttd1.0版本不支持服务器内部消息持久化，这是一个架构设计选择。首先，emqttd解决的核心问题是连接与路由；其次，我们认为内置持久化是个错误设计。

传统内置消息持久化的MQ服务器，比如广泛使用的JMS服务器ActiveMQ，几乎每个大版本都在重新设计持久化部分。内置消息持久化在设计上有两个问题:

1. 如何平衡内存与磁盘使用？消息路由基于内存，消息存储是基于磁盘。

2. 多服务器分布集群架构下，如何放置Queue？如何复制Queue的消息？

Kafka在上述问题上，做出了正确的设计：一个完全基于磁盘分布式commit log的消息服务器。

emqttd2.0版本计划通过外部存储，例如Redis、Kafka、Cassandra、PostgreSQL，实现多种方式的消息持久化。

设计上分离消息路由与消息存储职责后，数据复制容灾备份甚至应用集成，可以在数据层面灵活实现。

NetSplit Problem
----------------

The emqttd broker cluster requires reliable network to avoid NetSplit.

emqttd1.0消息服务器集群，基于Mnesia数据库设计。NetSplit发生时，节点间状态是：Erlang节点间可以连通，互相询问自己是否宕机，对方回答你已经宕机:(

NetSplit故障发生时，emqttd消息服务器的log/emqttd_error.log日志，会打印critical级别日志::

    Mnesia inconsistent_database event: running_partitioned_network, emqttd@host

emqttd集群部署在同一IDC网络下，NetSplit发生的几率很低，一旦发生又很难自动处理。所以emqttd1.0版本设计选择是，集群不自动化处理NetSplit，需要人工重启部分节点。

.. _design_architecture:

------------
Architecture
------------

Concept Model
-------------

emqttd消息服务器概念上更像一台网络路由器(Router)或交换机(Switch)，而不是传统的企业级消息服务器(MQ)。相比网络路由器按IP地址或MPLS标签路由报文，emqttd按主题树(Topic Trie)发布订阅模式在集群节点间路由MQTT消息:

.. image:: _static/images/concept.png

Design Principle
----------------

1. emqttd消息服务器核心解决的问题：处理海量的并发MQTT连接与路由消息。

2. 充分利用Erlang/OTP平台软实时、低延时、高并发、分布容错的优势。

3. 连接(Connection)、会话(Session)、路由(Router)、集群(Cluster)分层。

4. 消息路由平面(Flow Plane)与控制管理平面(Control Plane)分离。

5. 支持后端数据库或NoSQL实现数据持久化、容灾备份与应用集成。

System Layers
-------------

1. Network Layer: 负责TCP连接处理、MQTT协议编解码。

2. Session Layer: 处理MQTT协议发布订阅消息交互流程。
   
3. PubSub Layer: 节点内路由派发MQTT消息。
   
4. Route Layer: 分布节点间路由MQTT消息。
   
5. Authentication and ACL: 连接层支持可扩展的认证与访问控制模块。

6. Hooks and Plugins: 系统每层提供可扩展的钩子，支持插件方式扩展服务器。

-------------
Network Layer
-------------

连接层处理服务端Socket连接与MQTT协议编解码：

1. 基于 `eSockd`_ 框架的异步TCP服务端
2. TCP Acceptor池与异步TCP Accept
3. TCP/SSL, WebSocket/SSL连接支持
4. 最大并发连接数限制
5. 基于IP地址(CIDR)访问控制
6. 基于Leaky Bucket的流控
7. MQTT协议编解码
8. MQTT协议心跳检测
9. MQTT协议报文处理

* General Non-blocking TCP/SSL Socket Server
* Acceptor Pool and Asynchronous TCP Accept
* Parameterized Connection Module
* Max connections management
* Allow/Deny by peer address or CIDR
* Keepalive Support
* Rate Limit by Leaky Bucket
The_Leaky_Bucket_Algorithm

-------------
Session Layer
-------------

会话层处理MQTT协议发布订阅(Publish/Subscribe)业务交互流程：

1. 缓存MQTT客户端的全部订阅(Subscription)，并终结订阅QoS

2. 处理Qos0/1/2消息接收与下发，消息超时重传与离线消息保存

3. 飞行窗口(Inflight Window)，下发消息吞吐控制与顺序保证

4. 保存服务器发送到客户端的，已发送未确认的Qos1/2消息

5. 缓存客户端发送到服务端，未接收到PUBREL的QoS2消息

6. 客户端离线时，保存持久会话的离线Qos1/2消息

%% @doc Session for persistent MQTT client.
%%
%% Session State in the broker consists of:
%%
%% 1. The Client’s subscriptions.
%%
%% 2. inflight qos1/2 messages sent to the client but unacked, QoS 1 and QoS 2
%%    messages which have been sent to the Client, but have not been completely
%%    acknowledged.
%%
%% 3. inflight qos2 messages received from client and waiting for pubrel. QoS 2
%%    messages which have been received from the Client, but have not been
%%    completely acknowledged.
%%
%% 4. all qos1, qos2 messages published to when client is disconnected.
%%    QoS 1 and QoS 2 messages pending transmission to the Client.
%%
%% 5. Optionally, QoS 0 messages pending transmission to the Client.
%%
%% State of Message:  newcome, inflight, pending


MQueue and Inflight Window
---------------------------

%% Concept of Message Queue and Inflight Window:
%%
%%       |<----------------- Max Len ----------------->|
%%       -----------------------------------------------
%% IN -> |      Messages Queue   |  Inflight Window    | -> Out
%%       -----------------------------------------------
%%                               |<---   Win Size  --->|
%%
%%
%% 1. Inflight Window to store the messages delivered and awaiting for puback.
%%
%% 2. Enqueue messages when the inflight window is full.
%%
%% 3. If the queue is full, dropped qos0 messages if store_qos0 is true,
%%    otherwise dropped the oldest one.

会话层通过一个内存消息队列和飞行窗口处理下发消息::

       |<----------------- Max Len ----------------->|
       -----------------------------------------------
 IN -> |      Messages Queue   |  Inflight Window    | -> Out
       -----------------------------------------------
                               |<---   Win Size  --->|

飞行窗口(Inflight Window)保存当前正在发送未确认的Qos1/2消息。窗口值越大，吞吐越高；窗口值越小，消息顺序越严格。

当客户端离线或者飞行窗口(Inflight Window)满时，消息缓存到队列。如果消息队列满，先丢弃Qos0消息或最早进入队列的消息。

PacketId and MessageId
----------------------


MQTT协议定义了一个16bits的报文ID(PacketId)，用于客户端到服务器的报文收发与确认。MQTT发布报文(PUBLISH)进入消息服务器后，转换为一个消息对象并分配128bits消息ID(MessageId)。

The 128bits global unique messsage id::

    --------------------------------------------------------
    |        Timestamp       |  NodeID + PID  |  Sequence  | 
    |<------- 64bits ------->|<--- 48bits --->|<- 16bits ->|
    --------------------------------------------------------

    1. Timestamp: erlang:system_time if Erlang >= R18, otherwise os:timestamp
    2. NodeId:    encode node() to 2 bytes integer
    3. Pid:       encode pid to 4 bytes integer
    4. Sequence:  2 bytes sequence in one process


端到端消息发布订阅(Pub/Sub)过程中，发布报文ID与报文QoS终结在会话层，由唯一ID标识的MQTT消息对象在节点间路由::

    PktId <-- Session --> MsgId <-- Router --> MsgId <-- Session --> PktId

------------
PubSub Layer
------------

The PubSub layer would maintain a subscription table and publish MQTT messages to subscribers.

.. image:: _static/images/dispatch.png

MQTT messages will be dispatched to the subscriber's session, and finally be delivered to the client.

-----------
Route Layer
-----------

The route layer would maintain and replicate the global Topic Trie and Routing Table. The topic tire is composed of wildcard topics created by subscribers, and the Routing Table map a topic to nodes in the cluster.

For example, if node1 subscribed 't/+/x' and 't/+/y', node2 subscribed 't/#' and node3 subscribed 't/a', there will be a topic trie and route table::

    -------------------------
    |            t          |
    |           / \         |
    |          +   #        |
    |        /  \           |
    |      x      y         |
    -------------------------
    | t/+/x -> node1, node3 |
    | t/+/y -> node1        |
    | t/#   -> node2        |
    | t/a   -> node3        |
    -------------------------

The route layer would route MQTT messages between nodes in a cluster by topic trie match and routing table lookup.

.. image:: _static/images/route.png

## Cluster Design

1. One 'disc_copies' node and many 'ram_copies' nodes.

   2. Topic trie tree will be copied to every clusterd node.

   3. Subscribers to topic will be stored in each node and will not be copied.

   ## Cluster Strategy

   TODO:...

   1. A message only gets forwarded to other cluster nodes if a cluster node is interested in it. this reduces the network traffic tremendously, because it prevents nodes from forwarding unnecessary messages.

   2. As soon as a client on a node subscribes to a topic it becomes known within the cluster. If one of the clients somewhere in the cluster is publishing to this topic, the message will be delivered to its subscriber no matter to which cluster node it is connected.


.. _design_auth_acl:

----------------------
Authentication and ACL
----------------------

emqttd消息服务器支持可扩展的认证与访问控制，由emqttd_access_control、emqttd_auth_mod和emqttd_acl_mod模块实现。

emqttd_access_control模块提供了注册认证扩展接口::

    register_mod(auth | acl, atom(), list()) -> ok | {error, any()}.

    register_mod(auth | acl, atom(), list(), non_neg_integer()) -> ok | {error, any()}.

Authentication Bahavihour
-------------------------

emqttd_auth_mod定义认证扩展模块Behavihour::

    -module(emqttd_auth_mod).

    -ifdef(use_specs).

    -callback init(AuthOpts :: list()) -> {ok, State :: any()}.

    -callback check(Client, Password, State) -> ok | ignore | {error, string()} when
        Client    :: mqtt_client(),
        Password  :: binary(),
        State     :: any().

    -callback description() -> string().

    -else.

    -export([behaviour_info/1]).

    behaviour_info(callbacks) ->
        [{init, 1}, {check, 3}, {description, 0}];
    behaviour_info(_Other) ->
        undefined.

    -endif.

emqttd消息服务器自身实现的认证模块包括:

+-----------------------+--------------------------------+
| 模块                  | 认证方式                       |
+-----------------------+--------------------------------+
| emqttd_auth_username  | 用户名密码认证                 |
+-----------------------+--------------------------------+
| emqttd_auth_clientid  | ClientID认证                   |
+-----------------------+--------------------------------+
| emqttd_auth_ldap      | LDAP认证                       |
+-----------------------+--------------------------------+
| emqttd_auth_anonymous | 匿名认证                       |
+-----------------------+--------------------------------+

Authorization(ACL)
------------------

emqttd_acl_mod模块定义访问控制Behavihour::

    -module(emqttd_acl_mod).

    -include("emqttd.hrl").

    -ifdef(use_specs).

    -callback init(AclOpts :: list()) -> {ok, State :: any()}.

    -callback check_acl({Client, PubSub, Topic}, State :: any()) -> allow | deny | ignore when
        Client   :: mqtt_client(),
        PubSub   :: pubsub(),
        Topic    :: binary().

    -callback reload_acl(State :: any()) -> ok | {error, any()}.

    -callback description() -> string().

    -else.

    -export([behaviour_info/1]).

    behaviour_info(callbacks) ->
        [{init, 1}, {check_acl, 2}, {reload_acl, 1}, {description, 0}];
    behaviour_info(_Other) ->
        undefined.

    -endif.

emqttd_acl_internal模块实现缺省的基于etc/acl.config文件的访问控制::

    %%%-----------------------------------------------------------------------------
    %%%
    %%% -type who() :: all | binary() |
    %%%                {ipaddr, esockd_access:cidr()} |
    %%%                {client, binary()} |
    %%%                {user, binary()}.
    %%%
    %%% -type access() :: subscribe | publish | pubsub.
    %%%
    %%% -type topic() :: binary().
    %%%
    %%% -type rule() :: {allow, all} |
    %%%                 {allow, who(), access(), list(topic())} |
    %%%                 {deny, all} |
    %%%                 {deny, who(), access(), list(topic())}.
    %%%
    %%%-----------------------------------------------------------------------------

    {allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}.

    {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}.

    {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}.

    {allow, all}.

.. _design_hook:

------------
Hooks Design
------------

What's Hook
-----------

emqttd消息服务器在客户端上下线、主题订阅、消息收发位置设计了扩展钩子(Hook):

+------------------------+----------------------------------+
| 钩子                   | 说明                             |
+========================+==================================+
| client.connected       | 客户端上线                       |
+------------------------+----------------------------------+
| client.subscribe       | 客户端订阅主题前                 |
+------------------------+----------------------------------+
| client.subscribe.after | 客户端订阅主题后                 |
+------------------------+----------------------------------+
| client.unsubscribe     | 客户端取消订阅主题               |
+------------------------+----------------------------------+
| message.publish        | MQTT消息发布                     |
+------------------------+----------------------------------+
| message.delivered      | MQTT消息送达                     |
+------------------------+----------------------------------+
| message.acked          | MQTT消息回执                     |
+------------------------+----------------------------------+
| client.disconnected    | 客户端连接断开                   |
+------------------------+----------------------------------+

钩子(Hook)采用职责链设计模式(`Chain-of-responsibility_pattern`_)，扩展模块或插件向钩子注册回调函数，系统在客户端上下线、主题订阅或消息发布确认时，触发钩子顺序执行回调函数::

                     --------  ok | {ok, NewAcc}   --------  ok | {ok, NewAcc}   --------
     (Args, Acc) --> | Fun1 | -------------------> | Fun2 | -------------------> | Fun3 | --> {ok, Acc} | {stop, Acc}
                     --------                      --------                      --------
                        |                             |                             |
                   stop | {stop, NewAcc}         stop | {stop, NewAcc}         stop | {stop, NewAcc}

不同钩子的回调函数输入参数不同，用户可参考插件模版的 `emqttd_plugin_template`_ 模块，每个回调函数应该返回:

+-----------------+------------------------+
| 返回            | 说明                   |
+=================+========================+
| ok              | 继续执行               |
+-----------------+------------------------+
| {ok, NewAcc}    | 返回累积参数继续执行   |
+-----------------+------------------------+
| stop            | 停止执行               |
+-----------------+------------------------+
| {stop, NewAcc}  | 返回累积参数停止执行   |
+-----------------+------------------------+

Hook Implementation
-------------------

emqttd模块封装了Hook接口:

.. code:: erlang

    -module(emqttd).

    %% Hooks API
    -export([hook/4, hook/3, unhook/2, run_hooks/3]).
    hook(Hook :: atom(), Callback :: function(), InitArgs :: list(any())) -> ok | {error, any()}.

    hook(Hook :: atom(), Callback :: function(), InitArgs :: list(any()), Priority :: integer()) -> ok | {error, any()}.

    unhook(Hook :: atom(), Callback :: function()) -> ok | {error, any()}.

    run_hooks(Hook :: atom(), Args :: list(any()), Acc :: any()) -> {ok | stop, any()}.

emqttd_hook模块实现Hook机制:

.. code:: erlang

    -module(emqttd_hook).

    %% Hooks API
    -export([add/3, add/4, delete/2, run/3, lookup/1]).

    add(HookPoint :: atom(), Callback :: function(), InitArgs :: list(any())) -> ok.

    add(HookPoint :: atom(), Callback :: function(), InitArgs :: list(any()), Priority :: integer()) -> ok.

    delete(HookPoint :: atom(), Callback :: function()) -> ok.

    run(HookPoint :: atom(), Args :: list(any()), Acc :: any()) -> any().

    lookup(HookPoint :: atom()) -> [#callback{}].

Use Hooks
---------

`emqttd_plugin_template`_ 提供了全部钩子的使用示例，例如端到端的消息处理回调:

.. code:: erlang

    -module(emqttd_plugin_template).

    -export([load/1, unload/0]).
    
    -export([on_message_publish/2, on_message_delivered/3, on_message_acked/3]).

    load(Env) ->
        emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
        emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
        emqttd:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]).

    on_message_publish(Message, _Env) ->
        io:format("publish ~s~n", [emqttd_message:format(Message)]),
        {ok, Message}.

    on_message_delivered(ClientId, Message, _Env) ->
        io:format("delivered to client ~s: ~s~n", [ClientId, emqttd_message:format(Message)]),
        {ok, Message}.

    on_message_acked(ClientId, Message, _Env) ->
        io:format("client ~s acked: ~s~n", [ClientId, emqttd_message:format(Message)]),
        {ok, Message}.

    unload() ->
        emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
        emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/3),
        emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/3).

.. _design_plugin:

-------------
Plugin Design
-------------

插件是一个普通的Erlang应用(Application)，放置在emqttd/plugins目录可以被动态加载。插件主要通过钩子(Hook)机制扩展服务器功能，或通过注册扩展模块方式集成认证访问控制。

emqttd_plugins模块实现插件机制，提供加载卸载插件API::

    -module(emqttd_plugins).

    -export([load/1, unload/1]).

    %% @doc Load a Plugin
    load(PluginName :: atom()) -> ok | {error, any()}.

    %% @doc UnLoad a Plugin
    unload(PluginName :: atom()) -> ok | {error, any()}.

用户可通过'./bin/emqttd_ctl'命令行加载卸载插件::

    ./bin/emqttd_ctl plugins load emqttd_plugin_redis

    ./bin/emqttd_ctl plugins unload emqttd_plugin_redis

开发者请参考模版插件: http://github.com/emqtt/emqttd_plugin_template

.. _design_erlang:

----------
Erlang/OTP 
----------

1. Pool, Pool, Pool... Use the awesome GProc libary: https://github.com/uwiger/gproc

2. 异步，异步，异步消息...连接层到路由层异步消息，同步请求用于负载保护

3. 避免进程Mailbox累积消息，负载高的进程可以使用gen_server2

4. 消息流经的Socket连接、会话进程必须Hibernate，主动回收binary句柄

5. 多使用Binary数据，避免进程间内存复制

6. 使用ETS, ETS, ETS...Message Passing Vs ETS

7. 避免ETS表非键值字段select, match

8. 避免大量数据ETS读写, 每次ETS读写会复制内存，可使用lookup_element, update_counter

9. 适当开启ETS表{write_concurrency, true}

10. 保护Mnesia数据库事务，尽量减少事务数量，避免事务过载(overload)

11. 避免Mnesia数据表索引，和非键值字段match, select


---------------
Pubsub Sequence
---------------

## PubSub Sequence

### Clean Session = 1

```

title PubSub Sequence(Clean Session = 1)

ClientA-->PubSub: Publish Message
PubSub-->ClientB: Dispatch Message
```

![PubSub_CleanSess_1](http://emqtt.io/static/img/design/PubSub_CleanSess_1.png)

### Clean Session = 0

```
title PubSub Sequence(Clean Session = 0)

ClientA-->SessionA: Publish Message
SessionA-->PubSub: Publish Message
PubSub-->SessionB: Dispatch Message
SessionB-->ClientB: Dispatch Message

```
![PubSub_CleanSess_0](http://emqtt.io/static/img/design/PubSub_CleanSess_0.png)


## Qos

PubQos | SubQos | In Message | Out Message
-------|--------|------------|-------------
0   |   0    |   0        | 0
0   |   1    |   0        | 0
0   |   2    |   0        | 0
1   |   0    |   1        | 0
1   |   1    |   1        | 1
1   |   2    |   1        | 1
2   |   0    |   2        | 0
2   |   1    |   2        | 1
2   |   2    |   2        | 2


## Topic Functions Benchmark

Mac Air(11):

Function     | Time(microseconds)
-------------|--------------------
match        | 6.25086
triples      | 13.86881
words        | 3.41177
binary:split | 3.03776

iMac:

Function     | Time(microseconds)
-------------|--------------------
match        | 3.2348
triples      | 6.93524
words        | 1.89616
binary:split | 1.65243


--------------
Cluster Design
--------------

   ....

## Cluster Architecture

![Cluster Design](http://emqtt.io/static/img/Cluster.png)
## Cluster Command

```sh
./bin/emqttd_ctl cluster DiscNode
```

## Mnesia Example

```
(emqttd3@127.0.0.1)3> mnesia:info().
---> Processes holding locks <---
---> Processes waiting for locks <---
---> Participant transactions <---
---> Coordinator transactions <---
---> Uncertain transactions <---
---> Active tables <---
mqtt_retained : with 6 records occupying 221 words of mem
topic_subscriber: with 0 records occupying 305 words of mem
topic_trie_node: with 129 records occupying 3195 words of mem
topic_trie : with 128 records occupying 3986 words of mem
topic : with 93 records occupying 1797 words of mem
schema : with 6 records occupying 1081 words of mem
===> System info in version "4.12.4", debug level = none <===
opt_disc. Directory "/Users/erylee/Projects/emqttd/rel/emqttd3/data/mnesia" is NOT used.
use fallback at restart = false
running db nodes = ['emqttd2@127.0.0.1','emqttd@127.0.0.1','emqttd3@127.0.0.1']
stopped db nodes = []
master node tables = []
remote = []
ram_copies = [mqtt_retained,schema,topic,topic_subscriber,topic_trie,
topic_trie_node]
disc_copies = []
disc_only_copies = []
[{'emqttd2@127.0.0.1',ram_copies},
{'emqttd3@127.0.0.1',ram_copies},
{'emqttd@127.0.0.1',disc_copies}] = [schema]
[{'emqttd2@127.0.0.1',ram_copies},
{'emqttd3@127.0.0.1',ram_copies},
{'emqttd@127.0.0.1',ram_copies}] = [topic,topic_trie,topic_trie_node,
mqtt_retained]
[{'emqttd3@127.0.0.1',ram_copies}] = [topic_subscriber]
44 transactions committed, 5 aborted, 0 restarted, 0 logged to disc
   0 held locks, 0 in queue; 0 local transactions, 0 remote
   0 transactions waits for other nodes: []
   ```

   ## Cluster vs Bridge

   Cluster will copy topic trie tree between nodes, Bridge will not.



-------------
Hooks Design
-------------

## Overview 

emqttd supported a simple hooks mechanism in 0.8.0 release to extend the broker. The designed is improved in 0.9.0 release.

## API

emqttd_broker Hook API:

```
-export([hook/3, unhook/2, foreach_hooks/2, foldl_hooks/3]).
```

### Hook 

``` 
-spec hook(Hook :: atom(), Name :: any(), MFA :: mfa()) -> ok | {error, any()}.
hook(Hook, Name, MFA) ->
    ...
    ```

 ### Unhook

 ```
 -spec unhook(Hook :: atom(), Name :: any()) -> ok | {error, any()}.
 unhook(Hook, Name) ->
     ...
     ```

  ### Foreach Hooks

  ```
  -spec foreach_hooks(Hook :: atom(), Args :: list()) -> any().
  foreach_hooks(Hook, Args) ->
      ...
      ```

   ### Foldl Hooks

   ```
   -spec foldl_hooks(Hook :: atom(), Args :: list(), Acc0 :: any()) -> any().
   foldl_hooks(Hook, Args, Acc0) ->
       ...
       ```

    ## Hooks 

    Name             | Type      | Description
    ---------------  | ----------| --------------
    client.connected | foreach   | Run when client connected successfully
    client.subscribe | foldl     | Run before client subscribe topics
    client.subscribe.after | foreach | Run After client subscribe topics
    client.unsubscribe | foldl   | Run when client unsubscribe topics
    message.publish   | foldl     | Run when message is published
    message.acked   | foreach     | Run when message is acked
    client.disconnected | foreach | Run when client is disconnnected

    ## End-to-End Message Pub/Ack

    Could use 'message.publish', 'message.acked' hooks to implement end-to-end message pub/ack:

    ```
     PktId <-- --> MsgId <-- --> MsgId <-- --> PktId
          |<--- Qos --->|<---PubSub--->|<-- Qos -->|
          ```
## Limit

The design is experimental.

-------------
Plugin Design
-------------

## Overview

**Notice that 0.11.0 release use rebar to manage plugin's deps.**

A plugin is just an erlang application that extends emqttd broker.

The plugin application should be put in "emqttd/plugins/" folder to build. 


## Plugin Project

You could create a standalone plugin project outside emqttd, and then add it to "emqttd/plugins/" folder by "git submodule". 

Git submodule to compile emqttd_dashboard plugin with the broker, For example:

```
git submodule add https://github.com/emqtt/emqttd_dashboard.git plugins/emqttd_dashboard
make && make dist
```

## plugin.config

**Each plugin should have a 'etc/plugin.config' file**

For example, project structure of emqttd_dashboard plugin:

```
LICENSE
README.md
ebin
etc
priv
rebar.config
src
```

etc/plugin.config for emqttd_dashboard plugin:

```
[
{emqttd_dashboard, [
{listener,
{emqttd_dashboard, 18083, [
{acceptors, 4},
{max_clients, 512}]}}
]}
].
```

## rebar.config

**Plugin should use 'rebar.config' to manage depencies**

emqttd_plugin_pgsql plugin's rebar.config, for example:

```
%% -*- erlang -*-

{deps, [
{epgsql, ".*",{git, "https://github.com/epgsql/epgsql.git", {branch, "master"}}}
]}.
```

## Build emqttd with plugins

Put all the plugins you required in 'plugins/' folder of emqttd project, and then:

```
make && make dist
```

## Load Plugin

'./bin/emqttd_ctl' to load/unload plugin, when emqttd broker started.

```
./bin/emqttd_ctl plugins load emqttd_plugin_demo

./bin/emqttd_ctl plugins unload emqttd_plugin_demo
```

## List Plugins

```
./bin/emqttd_ctl plugins list
```

## API

```
%% Load all active plugins after broker started
emqttd_plugins:load() 

%% Load new plugin
emqttd_plugins:load(Name)

%% Unload all active plugins before broker stopped
emqttd_plugins:unload()

%% Unload a plugin
emqttd_plugins:unload(Name)
```

.. _eSockd: https://github.com/emqtt/esockd
.. _Chain-of-responsibility_pattern: https://en.wikipedia.org/wiki/Chain-of-responsibility_pattern
.. _emqttd_plugin_template: https://github.com/emqtt/emqttd_plugin_template/blob/master/src/emqttd_plugin_template.erl


