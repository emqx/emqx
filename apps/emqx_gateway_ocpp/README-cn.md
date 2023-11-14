# emqx-ocpp

OCPP-J 1.6 协议的 Central System 实现。

## 客户端信息映射

在 EMQX 4.x 中，OCPP-J 网关作为协议插件或协议模块（仅企业版本）进行提供。

所有连接到 OCPP-J 网关的 Charge Point，都会被当做一个普通的客户端对待（就像 MQTT 客户端一样）。
即可以使用 Charge Point 的唯一标识，在 Dashboard/HTTP-API/CLI 来管理它。

客户端信息的映射关系为：
- Client ID：Charge Point 的唯一标识。
- Username：从 HTTP Basic 认证中的 Username 解析得来。
- Password：从 HTTP Basic 认证中的 Password 解析得来。

### 认证

正如 **ocpp-j-1.6** 规范中提到的，Charge Point 可以使用 HTTP Basic 进行认证。
OCPP-J 网关从中提取 Username 和 Password，并通过 EMQX 的认证系统获取登录权限。

也就是说，OCPP-J 网关使用 EMQX 的认证插件来授权 Charge Point 的登录。

## 消息拓扑

```
                                +----------------+  upstream publish  +---------+
+--------------+   Req/Resp     | OCPP-J Gateway | -----------------> | Third   |
| Charge Point | <------------> | over           |     over Topic     | Service |
+--------------+   over ws/wss  | EMQX           | <----------------- |         |
                                +----------------+  dnstream publish  +---------+
```
Charge Point 和 OCPP-J 网关通过 OCPP-J 协议定义的规范进行通信。这主要是基于 Websocket 和 Websocket TLS

### Up Stream (emqx-ocpp -> third-services)

OCPP-J 网关将 Charge Point 所有的消息、事件通过 EMQX 进行发布。这个数据流称为 **Up Stream**。

其主题配置支持按任意格式进行配置，例如：
```
## 上行默认主题。emqx-ocpp 网关会将所有 Charge Point 的消息发布到该主题上。
##
## 可用占位符为：
## - cid: Charge Point ID
## - action: The Message Name for OCPP
##
ocpp.upstream.topic = ocpp/cp/${cid}/${action}

## 支持按消息名称对默认主题进行重载
##
ocpp.upstream.topic.BootNotification = ocpp/cp/${cid}/Notify/${action}
```
Payload 为固定格式，它包括字段

| Field             | Type        | Seq | Required | Desc |
| ----------------- | ----------- | --- | -------- | ---- |
| MessageTypeId     | MessageType | 1   | R        | Define the type of Message, whether it is Call, CallResult or CallError |
| UniqueId          | String      | 2   | R        | This must be the exact same id that is in the call request so that the recipient can match request and result |
| Action            | String      | 3   | O        | The Message Name of OCPP. E.g. Authorize |
| ErrorCode         | ErrorType   | 4   | O        | The string must contain one from ErrorType Table |
| ErrorDescription  | String      | 5   | O        | Detailed Error information |
| Payload           | Bytes       | 6   | O        | Payload field contains the serialized strings of bytes for protobuf format of OCPP message |

例如，一条在 upstream 上的 BootNotifiaction.req 的消息格式为：

```
Topic: ocpp/cp/CP001/Notify/BootNotifiaction
Payload:
  {"MessageTypeId": 2,
   "UniqueId": "1",
   "Payload": {"chargePointVendor":"vendor1","chargePointModel":"model1"}
  }
```

同样，对于 Charge Point 发送到 Central System 的 `*.conf` 的应答消息和错误通知，
也可以定制其主题格式：

```
ocpp.upstream.reply_topic = ocpp/cp/Reply/${cid}

ocpp.upstream.error_topic = ocpp/cp/Error/${cid}
```

注：Up Stream 消息的 QoS 等级固定为 2，即最终接收的 QoS 等级取决于订阅者发起订阅时的 QoS 等级。

### Down Stream (third-services -> emqx-ocpp)

OCPP-J 网关通过向 EMQX 订阅主题来接收控制消息，并将它转发的对应的 Charge Point，以达到消息下发的效果。
这个数据流被称为 **Down Stream**。

其主题配置支持按任意格式进行配置，例如：
```
## 下行主题。网关会为每个连接的 Charge Point 网关自动订阅该主题，
## 以接收下行的控制命令等。
##
## 可用占位符为：
## - cid: Charge Point ID
##
## 注：1. 为了区分每个 Charge Point，所以 ${cid} 是必须的
##     2. 通配符 `+` 不是必须的，此处仅是一个示例
ocpp.dnstream.topic = ocpp/${cid}/+/+
```

Payload 为固定格式，格式同 upstream。

例如，一条从 Third-Service 发到网关的 BootNotifaction 的应答消息格式为：
```
Topic: ocpp/cp/CP001/Reply/BootNotification
Payload:
  {"MessageTypeId": 3,
   "UniqueId": "1",
   "Payload": {"currentTime": "2022-06-21T14:20:39+00:00", "interval": 300, "status": "Accepted"}
  }
```

### 消息收发机制

正如 OCPP-J 协议所说，Charge Point 和 Central System 在发送出一条请求消息（CALL）后，都必须等待该条消息被应答，或者超时后才能发送下一条消息。

网关在实现上，支持严格按照 OCPP-J 定义的通信逻辑执行，也支持不执行该项检查。
```
ocpp.upstream.strit_mode = false
ocpp.dnstream.strit_mode = false
```

当 `upstream.strit_mode = false` 时，**只要 Charge Point 有新的消息到达，都会被发布到 upsteam 的主题上。**
当 `dnstream.strit_mode = false` 时，**只要 Third-Party 有新的消息发布到 dnstream，都会被里面转发到 Charge Point 上。**

注：当前版本，仅支持 `strit_mode = false`

#### Up Stream (Charge Point -> emqx-ocpp)

当 `upstream.strit_mode = true` 时， OCPP-J 网关处理 Up Stream 的行为：
- 收到的请求消息会立马发布到 Up Stream 并保存起来，直到 Down Stream 上得到一个该消息的应答、或答超时后才会被移除。但应答和错误消息不会被暂存。
- 如果上一条请求消息没有被应答或超时，后续收到的请求消息都会被 OCPP-J 网关丢弃并回复一个 `SecurityError` 错误。但如果这两条请求消息相同，则会在 Up Stream 上被重新发布。
- 当请求消息被应答或超时后，才会处理下一条请求消息。
- Charge Point 发送的应答和错误消息会立马发布到 Up Stream，不会被暂存，也不会阻塞下一条应答和错误消息。

相关配置有：
```
# 上行请求消息，最大的应答等待时间
ocpp.upstream.awaiting_timeout = 30s
```
#### Down Stream (Third-services -> emqx-ocpp)

当 `upstream.strit_mode = true` 时，Down Stream 的行为：

- 下行请求消息会先暂存到网关，直到它被 Charge Point 应答。
- 多条下行请求消息会被暂存到网关的发送队列中，直到上一条请求消息被确认才会发布下一条请求消息。
- 下行的应答和错误消息，会尝试确认 Charge Point 发送的请求消息。无论是否确认成功，该消息都会立马投递到 Charge Point，并不会在消息队列里排队。
- 下行的请求消息不会被丢弃，如果等待超时则会重发该请求消息，直到它被确认。

相关配置有：
```
# 下行请求消息重试间隔
ocpp.dnstream.retry_interval = 30s

# 下行请求消息最大队列长度
ocpp.dnstream.max_mqueue_len = 10
```

### 消息格式检查

网关支持通过 Json-Schema 来校验每条消息 Payload 的合法性。

```
## 检查模式
#ocpp.message_format_checking = all

## json-schema 文件夹路径
#ocpp.json_schema_dir = ${application_priv}/schemas

## json-schema 消息前缀
#ocpp.json_schema_id_prefix = urn:OCPP:1.6:2019:12:
```
