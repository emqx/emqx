# 设计

## 动机

增强系统的扩展性。包含的目的有：

- 完全支持各种钩子，能够根据其返回值修改 EMQ X 或者 Client 的行为。
  - 例如 `auth/acl`：可以查询数据库或者执行某种算法校验操作权限。然后返回 `false` 表示 `认证/ACL` 失败。
  - 例如 `message.publish`：可以解析 `消息/主题` 并将其存储至数据库中。

- 支持多种语言的扩展；并包含该语言的示例程序。
  - python
  - webhook
  - Java
  - Lua
  - c，go，.....
- 热操作
  - 允许在插件运行过程中，添加和移除 `Driver`。

- 需要 CLI ，甚至 API 来管理 `Driver`

注：`message` 类钩子仅包括在企业版中。

## 设计

架构如下：

```
 EMQ X                                      Third-party Runtimes
+========================+                 +====================+
|    Extension           |                 |                    |
|   +----------------+   |     Hooks       |  Python scripts /  |
|   |    Drivers     | ------------------> |  Java Classes   /  |
|   +----------------+   |     (pipe)      |  Others ...        |
|                        |                 |                    |
+========================+                 +====================+
```

### 配置文件示例

#### 驱动 配置

```properties
## Driver type
##
## Exmaples:
##   - python3                   --- 仅配置 python3
##   - python3, java, webhook    --- 配置多个 Driver
exhook.dirvers = python3, java, webhook

## --- 具体 driver 的配置详情

## Python
exhook.dirvers.python3.path = data/extension/python
exhook.dirvers.python3.call_timeout = 5s
exhook.dirvers.python3.pool_size = 8

## java
exhook.drivers.java.path = data/extension/java
...
```

#### 钩子配置

钩子支持配置在配置文件中，例如：

```properties
exhook.rule.python3.client.connected = {"module": "client", "callback": "on_client_connected"}
exhook.rule.python3.message.publish  = {"module": "client", "callback": "on_client_connected", "topics": ["#", "t/#"]}
```

***已废弃！！（冗余）***


###  驱动抽象

#### APIs

| 方法名                   | 说明     | 入参   | 返回   |
| ------------------------ | -------- | ------ | ------ |
| `init`                   | 初始化   | -      | 见下表 |
| `deinit`                 | 销毁     | -      | -      |
| `xxx `*(由init函数定义)* | 钩子回调 | 见下表 | 见下表 |



##### init 函数规格

```erlang
%% init 函数
%% HookSpec 			: 为用户在脚本中的 初始化函数指定的；他会与配置文件中的内容作为默认值，进行合并
%%          			  该参数的目的，用于 EMQ X 判断需要执行哪些 Hook 和 如何执行 Hook
%% State    			: 为用户自己管理的数据内容，EMQ X 不关心它，只来回透传
init() -> {HookSpec, State}.

%% 例如：
{[{client_connect, callback_m(), callback_f(),#{}, {}}]}

%%--------------------------------------------------------------
%% Type Defines

-tpye hook_spec() :: [{hookname(), callback_m(), callback_f(), hook_opts()}].

-tpye state :: any().

-type hookname() :: client_connect
                  | client_connack
                  | client_connected
                  | client_disconnected
                  | client_authenticate
                  | client_check_acl
                  | client_subscribe
                  | client_unsubscribe
                  | session_created
                  | session_subscribed
                  | session_unsubscribed
                  | session_resumed
                  | session_discarded      %% TODO: Should squash to `terminated` ?
                  | session_takeovered     %% TODO: Should squash to `terminated` ?
                  | session_terminated
                  | message_publish
                  | message_delivered
                  | message_acked
                  | message_dropped.

-type callback_m() :: atom().          -- 回调的模块名称；python 为脚本文件名称；java 为类名；webhook 为 URI 地址

-type callback_f() :: atom().          -- 回调的方法名称；python，java 等为方法名；webhook 为资源地址

-tpye hook_opts() :: [{hook_key(), any()}].  -- 配置项；配置该项钩子的行为

-type hook_key() :: topics | ...
```



##### deinit 函数规格

``` erlang
%% deinit 函数；不关心返回的任何内容
deinit() -> any().
```



##### 回调函数规格

| 钩子                 | 入参                                                  | 返回      |
| -------------------- | ----------------------------------------------------- | --------- |
| client_connect       | `connifno`<br />`props`                               | -         |
| client_connack       | `connifno`<br />`rc`<br />`props`                     | -         |
| client_connected     | `clientinfo`<br />                                    | -         |
| client_disconnected  | `clientinfo`<br />`reason`                            | -         |
| client_authenticate  | `clientinfo`<br />`result`                            | `result`  |
| client_check_acl     | `clientinfo`<br />`pubsub`<br />`topic`<br />`result` | `result`  |
| client_subscribe     | `clientinfo`<br />`props`<br />`topicfilters`         | -         |
| client_unsubscribe   | `clientinfo`<br />`props`<br />`topicfilters`         | -         |
| session_created      | `clientinfo`                                          | -         |
| session_subscribed   | `clientinfo`<br />`topic`<br />`subopts`              | -         |
| session_unsubscribed | `clientinfo`<br />`topic`                             | -         |
| session_resumed      | `clientinfo`                                          | -         |
| session_discared     | `clientinfo`                                          | -         |
| session_takeovered   | `clientinfo`                                          | -         |
| session_terminated   | `clientinfo`<br />`reason`                            | -         |
| message_publish      | `messsage`                                            | `message` |
| message_delivered    | `clientinfo`<br />`message`                           | -         |
| message_dropped      | `message`                                             | -         |
| message_acked        | `clientinfo`<br />`message`                           | -         |



上表中包含数据格式为：

```erlang
-type conninfo :: [ {node, atom()}
                  , {clientid, binary()}
                  , {username, binary()}
                  , {peerhost, binary()}
							    , {sockport, integer()}
                  , {proto_name, binary()}
                  , {proto_ver, integer()}
                  , {keepalive, integer()}
								  ].

-type clientinfo :: [ {node, atom()}
                    , {clientid, binary()}
                    , {username, binary()}
                    , {password, binary()}
                    , {peerhost, binary()}
							      , {sockport, integer()}
                    , {protocol, binary()}
                    , {mountpoint, binary()}
                    , {is_superuser, boolean()}
                    , {anonymous, boolean()}
								    ].

-type message :: [ {node, atom()}
                 , {id, binary()}
                 , {qos, integer()}
                 , {from, binary()}
                 , {topic, binary()}
                 , {payload, binary()}
                 , {timestamp, integer()}
                 ].

-type rc :: binary().
-type props :: [{key(), value()}]

-type topics :: [topic()].
-type topic :: binary().
-type pubsub :: publish | subscribe.
-type result :: true | false.
```



### 统计

在驱动运行过程中，应有对每种钩子调用计数，例如：

```
exhook.python3.check_acl               10
```



### 管理

**CLI 示例：**



**列出所有的驱动**

```
./bin/emqx_ctl exhook dirvers list
Drivers(xxx=yyy)
Drivers(aaa=bbb)
```



**开关驱动**

```
./bin/emqx_ctl exhook drivers enable python3
ok

./bin/emqx_ctl exhook drivers disable python3
ok

./bin/emqx_ctl exhook drivers stats
python3.client_connect     123
webhook.check_acl          20
```
