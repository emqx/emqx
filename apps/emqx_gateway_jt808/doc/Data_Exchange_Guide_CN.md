# JT/T 808 2013 网关数据交换格式

该文档定义了 **emqx_jt808** 和 **EMQX** 之间数据交换的格式

约定:
- Payload 采用 Json 格式进行组装
- Json Key 采用全小写格式命名

Json 结构示例

## 终端到服务器
```json
{
  "header" : {
    "msg_id" : 1,
    "encrypt": 0,
    "len": VAL,
    "phone": 13900000000,
    "msg_sn": 0
  },
  "body": {
    "seq": 1,
    "id": 1,
    "result": 0
  }
}
```

## 服务器到终端
```json
{
  "header": {
    "msg_id": 32769,
    "encrypt": 0,
    "phone": 13900000000,
    "msg_sn": 0
  },
  "body": {
    "seq": 1,
    "id": 1,
    "result": 0
  }
}
```

## 数据类型对照表
| JT808 Defined Type | In Json Type | Comment    |
|:------------------:|:------------:|:----------:|
| BYTE               | integer      | in decimal |
| WORD               | integer      | in decimal |
| DWORD              | integer      | in decimal |
| BYTE(n)            | string       |            |
| BCD(n)             | string       |            |
| STRING             | string       |            |

## 字段对照表

### 消息头字段对照表

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 消息 ID      | msg_id        | word       | integer            |
| 数据加密方式 | encrypt       | word       | integer            |
| 终端手机号   | phone         | bcd(6)     | string             |
| 消息流水号   | msg_sn        | word       | integer            |

| Optional Field | Json Key name | Value Type | Value Type in JSON |
|:--------------:|:-------------:|:----------:|:------------------:|
| 消息总包数     | frag_total    | word       | integer            |
| 消息包序号     | frag_sn       | word       | integer            |

- 存在 `frag_total` 与 `frag_sn` 时表示消息体为长消息，进行分包处理

### 消息体字段对照表

#### 终端通用应答 `"msg_id": 1` 0x0001

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 应答流水号 | seq           | word       | integer            |
| 应答 ID    | id            | word       | integer            |
| 结果       | result        | byte       | integer            |


#### 平台通用应答 `"msg_id": 32769` 0x8001

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 应答流水号 | seq           | word       | integer            |
| 应答 ID    | id            | word       | integer            |
| 结果       | result        | byte       | integer            |


#### 终端心跳 `"msg_id": 2` 0x0002

空 Json


#### 补传分包请求 `"msg_id": 32771` 0x8003

| Field          | Json Key name | Value Type     | Value Type in Json |
|:--------------:|:-------------:|:--------------:|:------------------:|
| 原始消息流水号 | seq           | word           | integer            |
| 重传包总数     | length        | byte           | integer            |
| 重传包 ID 列表 | ids           | byte(2*length) | list of integer    |


#### 终端注册 `"msg_id": 256` 0x0100

| Field     | Json Key name  | Value Type | Value Type in Json |
|:---------:|:--------------:|:----------:|:------------------:|
| 省域 ID   | province       | word       | integer            |
| 市县域 ID | city           | word       | integer            |
| 制造商 ID | manufacture    | byte(5)    | string             |
| 终端型号  | model          | byte(20)   | string             |
| 终端 ID   | dev_id         | byte(7)    | string             |
| 车牌颜色  | color          | byte       | integer            |
| 车辆标识  | license_number | string     | string             |


#### 终端注册应答 `"msg_id": 33024` 0x8100

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 应答流水号 | seq           | word       | integer            |
| 结果       | result        | byte       | integer            |

只有成功后才有此字段

| Optional Field | Json Key name | Value Type | Value Type in JSON |
|:--------------:|---------------|------------|--------------------|
| 鉴权码         | auth_code     | string     | string             |


#### 终端注销 `"msg_id": 3` 0x0003

空 Json


#### 终端鉴权 `"msg_id": 258` 0x0102

| Field  | Json Key name | Value Type | Value Type in Json |
|:------:|:-------------:|:----------:|:------------------:|
| 鉴权码 | code          | string     | string             |


#### 设置终端参数 `"msg_id": 33027` 0x8103

| Field      | Json Key name | Value Type | Value Type in Json                                     |
|:----------:|:-------------:|:----------:|:------------------------------------------------------:|
| 参数总数   | length        | byte       | integer                                                |
| 参数项列表 | params        | list       | list of id and value. `[{"id":ID, "value": VAL}, ...]` |
| 参数项     | id            | dword      | integer                                                |
| 参数值     | value         | byte       | integer                                                |

参数 ID 说明见协议规定.


#### 查询终端参数 `"msg_id": 33028` 0x8104

空 Json


#### 查询指定终端参数 `"msg_id": 33030` 0x8106

| Field        | Json Key name | Value Type     | Value Type in Json               |
|:------------:|:-------------:|:--------------:|:--------------------------------:|
| 参数总数     | length        | byte           | integer                          |
| 参数 ID 列表 | ids           | byte(2*length) | list of id. `[1, 2, 3, 4, ...]` |

参数 ID 列表中元素为 integer


#### 查询终端应答参数 `"msg_id": 260` 0x0104

| Field        | Json Key name | Value Type | Value Type in Json                                     |
|:------------:|:-------------:|:----------:|:------------------------------------------------------:|
| 应答流水号   | seq           | word       | integer                                                |
| 应答参数个数 | length        | byte       | integer                                                |
| 参数项列表   | params        | list       | list of id and value. `[{"id":ID, "value": VAL}, ...]` |
| 参数项       | id            | dword      | integer                                                |
| 参数值       | value         | byte       | integer                                                |

参数 ID 说明见协议规定.


#### 终端控制 `"msg_id": 33029 ` 0x8105

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 命令字   | command       | byte       | integer            |
| 命令参数 | param         | string     | string             |


#### 查询终端属性 `"msg_id": 33031` 0x8107

空 Json


#### 查询终端属性应答 `"msg_id": 263` 0x0107

| Field             | Json Key name    | Value Type | Value Type in Json |
|:-----------------:|:----------------:|:----------:|:------------------:|
| 终端类型          | type             | word       | integer            |
| 制造商 ID         | manufacture      | byte(5)    | string             |
| 终端型号          | model            | byte(20)   | string             |
| 终端 ID           | id               | byte(7)    | string             |
| 终端 SIM 卡 ICCID | iccid            | byte(10)   | string             |
| 终端硬件版本号    | hardware_version | string     | string             |
| 终端硬件固件号    | firmware_version | string     | string             |
| GNSS 模块属性     | gnss_prop        | byte       | integer            |
| 通信模块属性      | comm_prop        | byte       | integer            |

- 终端硬件版本号长度、终端固件版本号长度，将被用于二进制报文解析，不向上暴露


#### 下发终端升级包 `"msg_id": 33032` 0x8108

| Field          | Json Key name | Value Type | Value Type in Json     |
|:--------------:|:-------------:|:----------:|:----------------------:|
| 升级类型       | type          | byte       | integer                |
| 制造商 ID      | manufacturer  | byte(5)    | string                 |
| 版本号长度     | ver_len       | byte       | integer                |
| 版本号         | version       | string     | string                 |
| 升级数据包长度 | fw_len        | dword      | integer                |
| 升级数据包     | firmware      | binary     | string(base64 encoded) |


#### 终端升级结果通知 `"msg_id": 264` 0x0108

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 升级类型 | type          | byte       | integer            |
| 升级结果 | result        | byte       | integer            |


#### 位置信息汇报 `"msg_id": 512` 0x0200

| Field                | Json Key name | Value Type | Value Type in Json |
|:--------------------:|:-------------:|:----------:|:------------------:|
| 报警标志             | alarm         | dword      | integer            |
| 状态                 | status        | dword      | integer            |
| 纬度                 | latitude      | dword      | integer            |
| 经度                 | longitude     | dword      | integer            |
| 高程                 | altitude      | word       | integer            |
| 速度                 | speed         | word       | integer            |
| 方向                 | direction     | word       | integer            |
| 时间                 | time          | bcd(6)     | string             |

| Optional Field     | Json Key name | Value Type | Value Type in JSON |
|:------------------:|:-------------:|:----------:|:------------------:|
| 位置附加信息项列表 | extra         | -          | map                |

<!-- TODO: refine alarm mroe details -->

- 位置附加信息项列表, 在 `extra` 中

| Field (附加信息描述)              | Json Key name   | Value Type | Value Type in Json     |
|:---------------------------------:|:---------------:|:----------:|:----------------------:|
| 里程                              | mileage         | dword      | integer                |
| 油量                              | fuel_meter      | word       | integer                |
| 行驶记录功能获取的速度            | speed           | word       | integer                |
| 需要人工确认报警事件的 ID         | alarm_id        | word       | integer                |
| 超速报警附加信息(长度1或5)        | overspeed_alarm | -          | map                    |
| 进出区域/路线报警附加信息         | in_out_alarm    | -          | map                    |
| 路段行驶时间不足/过长报警附加信息 | path_time_alarm | -          | map                    |
| 扩展车辆信号状态位                | 见状态位附表    | -          | -                      |
| IO 状态位                         | io_status       | -          | map                    |
| 模拟量                            | analog          | -          | map                    |
| 无线通信网络信号强度              | rssi            | byte       | integer                |
| GNSS 定位卫星数                   | gnss_sat_num    | byte       | integer                |
| 后续自定义信息长度                | custome         | -          | string(base64 encoded) |
| ## TODO 自定义区域                |                 |            |                        |

- 超速报警附加信息(长度1或5), 置于 map `overspeed_alarm` 内

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 位置类型 | type          | byte       | integer            |

| Optional Field | Json Key name | Value Type | Value Type in JSON |
|:--------------:|:-------------:|:----------:|:------------------:|
| 区域或路段 ID  | id            | dword      | integer            |


- 进出区域/路线报警附加信息, 置于 map `in_out_alarm` 内

| Field         | Json Key name | Value Type | Value Type in Json |
|:-------------:|:-------------:|:----------:|:------------------:|
| 位置类型      | type          | byte       | integer            |
| 区域或路段 ID | id            | dword      | integer            |
| 方向          | direction     | byte       | integer            |

- 路段行驶时间不足/过长报警附加信息, 置于 map `path_time_alarm` 内

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 路段 ID      | id            | dword      | integer            |
| 路段行驶时间 | time          | word       | integer            |
| 结果         | result        | byte       | integer            |

- IO 状态位, 置于 map `io_status` 内

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 深度休眠状态 | deep_sleep    | 1 bit      | integer            |
| 休眠状态     | sleep         | 1 bit      | integer            |

- 模拟量, 置于 map  `analog` 内

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 模拟量 0 | ad0           | 16 bits    | integer            |
| 模拟量 1 | ad1           | 16 bits    | integer            |

- 扩展车辆信号状态位, 置于 map `extra` 内

| Field        | Json Key name   | Value Type | Value Type in Json                         |
|:------------:|:---------------:|:----------:|:------------------------------------------:|
| 信号         | signal          | - 2 bits   | map, `{"low_beam": VAL, "high_beam": VAL}` |
| 右转向灯信号 | right_turn      | 1 bit      | integer                                    |
| 左转向灯信号 | left_turn       | 1 bit      | integer                                    |
| 制动信号     | brake           | 1 bit      | integer                                    |
| 倒档信号     | reverse         | 1 bit      | integer                                    |
| 雾灯信号     | fog             | 1 bit      | integer                                    |
| 示廓灯       | side_marker     | 1 bit      | integer                                    |
| 喇叭状态     | horn            | 1 bit      | integer                                    |
| 空调状态     | air_conditioner | 1 bit      | integer                                    |
| 空档信号     | neutral         | 1 bit      | integer                                    |
| 缓速器工作   | retarder        | 1 bit      | integer                                    |
| ABS 工作     | abs             | 1 bit      | integer                                    |
| 加热器工作   | heater          | 1 bit      | integer                                    |
| 离合器状态   | cluth           | 1 bit      | integer                                    |

- 信号状态, 置于 map `signal` 内

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 近光灯信号 | low_beam      | 1 bit      | integer            |
| 远光灯信号 | high_beam     | 1 bit      | integer            |

例
example:
```json
{
  "header" : {
    "msg_id" : 1,
    "encrypt": 0,
    "len": VAL,
    "phone": 13900000000,
    "msg_sn": 0
  },
  "body": {
    "alarm": VAL,
    "status": VAL,
    "latitude": VAL,
    "longitude": VAL,
    "altitude": VAL,
    "speed": VAL,
    "direction": VAL,
    "time": VAL,
    "extra": {
      "mileage": VAL,
      "fuel_unit": VAL,
      "speed": VAL,
      "alarm_id": VAL,
      "overspeed_alarm": {
        "type": VAL,
        "id": VAL
      },
      "in_out_alarm": {
        "type": VAL,
        "id": VAL,
        "direction": VAL
      },
      "path_time_alarm": {
        "id": VAL,
        "time": VAL,
        "result": VAL
      },
      "signal": {
        "low_beam": VAL,
        "high_beam": VAL
      },
      "right_turn": VAL,
      "left_turn": VAL,
      "break": VAL,
      "reverse": VAL,
      "fog": VAL,
      "side_marker": VAL,
      "horn": VAL,
      "air_conditioner": VAL,
      "neutral": VAL,
      "retarder": VAL,
      "abs": VAL,
      "heater": VAL,
      "cluth": VAL,
      "io_status": {
        "deep_sleep": VAL,
        "sleep": VAL
      },
      "analog": {
        "ad0": VAL,
        "ad1": VAL
      }
    }
  }
}
```


#### 位置信息查询 `"msg_id": 33281` 0x8201

空 Json


#### 位置信息查询应答 `"msg_id": 513` 0x0201

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 应答流水号   | seq           | word       | integer            |
| 位置信息汇报 | params        | -          | map                |


#### 临时位置跟踪控制 `"msg_id": 33282` 0x8202

| Field          | Json Key name | Value Type | Value Type in Json |
|:--------------:|:-------------:|:----------:|:------------------:|
| 时间间隔       | period        | word       | integer            |
| 跟踪位置有效期 | expiry        | dword      | integer            |


#### 人工确认报警消息 `"msg_id": 33283` 0x8203

| Field            | Json Key name | Value Type | Value Type in Json |
|:----------------:|:-------------:|:----------:|:------------------:|
| 报警消息流水号   | seq           | word       | integer            |
| 人工确认报警类型 | type          | dword      | integer            |


#### 文本信息下发 `"msg_id": 33536` 0x8300

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 标志     | flag          | byte       | integer            |
| 文本信息 | text          | string     | string             |


#### 事件设置 `"msg_id": 33537` 0x8301

| Field        | Json Key name | Value Type | Value Type in Json                                                |
|:------------:|:-------------:|:----------:|:-----------------------------------------------------------------:|
| 设置类型     | type          | byte       | integer                                                           |
| 设置总数     | length        | byte       | integer                                                           |
| 事件项列表   | events        | list       | list of event. `[{"id": ID, "length": LEN, "content": CON}, ...]` |
| 事件 ID      | id            | byte       | integer                                                           |
| 事件内容长度 | length        | byte       | integer                                                           |
| 事件内容     | content       | string     | string                                                            |


#### 事件报告 `"msg_id": 769` 0x0301

| Field   | Json Key name | Value Type | Value Type in Json |
|:-------:|:-------------:|------------|:------------------:|
| 事件 ID | id            | byte       | integer            |


#### 提问下发 `"msg_id": 33538` 0x8302

| Field        | Json Key name | Value Type | Value Type in Json                                             |
|:------------:|:-------------:|:----------:|:--------------------------------------------------------------:|
| 标志         | flag          | byte       | integer                                                        |
| 问题内容长度 | length        | byte       | integer                                                        |
| 问题         | question      | string     | string                                                         |
| 候选答案列表 | answers       | list       | list of answer. `[{"id": ID, "len": LEN, "answer": ANS}, ...]` |
| 答案 ID      | id            | byte       | integer                                                        |
| 答案内容长度 | len           | byte       | integer                                                        |
| 答案内容     | answer        | string     | string                                                         |

<!-- TODO: len -> length or other length -> len -->

#### 提问应答 `"msg_id": 770` 0x0302

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 应答流水号 | seq           | word       | integer            |
| 答案 ID    | id            | byte       | integer            |


#### 信息点播菜单设置 `"msg_id": 33539` 0x8303

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 设置类型     | type          | byte       | integer            |
| 信息项总数   | length        | byte       | integer            |
| 信息项列表   | menus         | list       | list of menu       |
| 信息类型     | type          | byte       | integer            |
| 信息名称长度 | length        | word       | integer            |
| 信息名称     | info          | string     | string             |


#### 信息点播/取消 `"msg_id": 771` 0x0303

| Field         | Json Key name | Value Type | Value Type in Json |
|:-------------:|:-------------:|:----------:|:------------------:|
| 信息类型      | id            | byte       | integer            |
| 点拨/取消标志 | flag          | byte       | integer            |


#### 信息服务 `"msg_id": 33540` 0x8304

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 信息类型 | type          | byte       | integer            |
| 信息长度 | length        | word       | integer            |
| 信息内容 | info          | string     | string             |


#### 电话回拨 `"msg_id": 33792` 0x8400

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 标志     | type          | byte       | integer            |
| 电话号码 | phone         | string     | string             |


#### 设置电话本 `"msg_id": 33793` 0x8401

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 设置类型   | type          | byte       | integer            |
| 联系人总数 | length        | byte       | integer            |
| 联系人项   | contacts      | list       | list of contact.   |
| 标志       | type          | byte       | integer            |
| 号码长度   | phone_len     | byte       | integer            |
| 电话号码   | phone         | string     | string             |
| 联系人长度 | name_len      | byte       | integer            |
| 联系人     | name          | string     | string             |

联系人项示例
```json
[{"type": TYPE, "phone_len", PH_LEN, "phone": PHONE, "name_len": NAME_LEN, "name": NAME}, ...]
```


#### 车辆控制 `"msg_id": 34048` 0x8500

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 标志控制 | flag          | byte       | integer            |


#### 车辆控制应答 `"msg_id": 1280` 0x0500

| Field              | Json Key name | Value Type | Value Type in Json |
|:------------------:|:-------------:|:----------:|:------------------:|
| 应答流水号         | seq           | word       | integer            |
| 位置信息汇报消息体 | location      | map        | map of location    |


#### 设置圆形区域 `"msg_id": 34304` 0x8600

| Field        | Json Key name      | Value Type | Value Type in Json |
|:------------:|:------------------:|:----------:|:------------------:|
| 设置属性     | type               | byte       | integer            |
| 区域总数     | length             | byte       | integer            |
| 区域项       | areas              | list       | list of area.     |
| 区域 ID      | id                 | dword      | integer            |
| 区域属性     | flag               | dword      | integer            |
| 中心点纬度   | center_latitude    | dword      | integer            |
| 中心点经度   | center_longitude   | dword      | integer            |
| 半径         | radius             | dword      | integer            |
| 起始时间     | start_time         | string     | string             |
| 结束时间     | end_time           | string     | string             |
| 最高速度     | max_speed          | word       | integer            |
| 超速持续时间 | overspeed_duration | byte       | integer            |

区域列表示例
```json
[{"id": ID,
   "flag": FLAG,
   "center_latitude": CEN_LAT,
   "center_longitude": CEN_LON,
   "radius": RADIUS,
   "start_time": START_TIME,
   "end_time": END_TIME,
   "max_speed", MAX_SPEED,
   "overspeed_duration", OVERSPEED_DURATION
   },
  ...
 ]
```


#### 删除圆形区域 `"msg_id": 34305` 0x8601

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 区域数       | length        | byte       | integer            |
| 区域 ID 列表 | ids           | list       | list of id.        |
| 区域 ID 1~n  | -             | dword      | integer            |

`[ID1, ID2, ...]`


#### 设置矩形区域 `"msg_id": 34306` 0x8602

| Field        | Json Key name      | Value Type | Value Type in Json       |
|:------------:|:------------------:|:----------:|:------------------------:|
| 设置属性     | type               | byte       | integer                  |
| 区域总数     | length             | byte       | integer                  |
| 区域项       | areas              | list       | list of rectangle area. |
| 区域 ID      | id                 | dword      | integer                  |
| 区域属性     | flag               | dword      | integer                  |
| 左上点纬度   | lt_lat             | dword      | integer                  |
| 左上点经度   | lt_lng             | dword      | integer                  |
| 右下点纬度   | rb_lat             | dword      | integer                  |
| 右下点经度   | rb_lng             | dword      | integer                  |
| 起始时间     | start_time         | string     | string                   |
| 结束时间     | end_time           | string     | string                   |
| 最高速度     | max_speed          | word       | integer                  |
| 超速持续时间 | overspeed_duration | byte       | integer                  |


#### 删除矩形区域 `"msg_id": 34307` 0x8603

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 区域数       | length        | byte       | integer            |
| 区域 ID 列表 | ids           | list       | list of id.        |
| 区域 ID 1~n  | -             | dword      | integer            |


#### 设置多边形区域 `"msg_id": 34308` 0x8604

| Field        | Json Key name      | Value Type | Value Type in Json |
|:------------:|:------------------:|:----------:|:------------------:|
| 区域 ID      | id                 | dword      | integer            |
| 区域属性     | flag               | dword      | integer            |
| 起始时间     | start_time         | string     | string             |
| 结束时间     | end_time           | string     | string             |
| 最高速度     | max_speed          | word       | integer            |
| 超速持续时间 | overspeed_duration | byte       | integer            |
| 区域总顶点数 | length             | word       | integer            |
| 顶点项列表   | points             | list       | list of point.     |
| 顶点纬度     | lat                | dword      | integer            |
| 顶点经度     | lng                | dword      | integer            |


#### 删除多边形区域 `"msg_id": 34309` 0x8605

| Field        | Json Key name | Value Type | Value Type in Json |
|:------------:|:-------------:|:----------:|:------------------:|
| 区域数       | length        | byte       | integer            |
| 区域 ID 列表 | ids           | list       | list of id.        |
| 区域 ID 1~n  | -             | dword      | integer            |


#### 设置路线 `"msg_id": 34310` 0x8606

| Field            | Json Key name      | Value Type | Value Type in Json |
|:----------------:|:------------------:|:----------:|:------------------:|
| 路线 ID          | id                 | dword      | integer            |
| 路线属性         | flag               | word       | integer            |
| 起始时间         | start_time         | string     | string             |
| 结束时间         | end_time           | string     | string             |
| 路线总拐点数     | length             | word       | integer            |
| 拐点项           | points             | list       | list of point.     |
| 拐点 ID          | point_id           | dword      | integer            |
| 路段 ID          | path_id            | dword      | integer            |
| 拐点纬度         | point_lat          | dword      | integer            |
| 拐点经度         | point_lng          | dword      | integer            |
| 路段宽度         | width              | byte       | integer            |
| 路段属性         | attrib             | byte       | integer            |
| 路段行驶过长阈值 | passed             | word       | integer            |
| 路段行驶不足阈值 | uncovered          | word       | integer            |
| 路段最高速度     | max_speed          | word       | integer            |
| 路段超速持续时间 | overspeed_duration | byte       | integer            |


#### 删除路线 `"msg_id": 34311` 0x8607

| Field    | Json Key name | Value Type | Value Type in Json |
|:--------:|:-------------:|:----------:|:------------------:|
| 路线数   | length        | byte       | integer            |
| 路线列表 | ids           | list       | list of id         |
| 路线 ID  | -             | dword      | integer            |


#### 行驶记录数据采集命令 `"msg_id": 34560` 0x8700

| Field  | Json Key name | Value Type             | Value Type in Json |
|:------:|:-------------:|:----------------------:|:------------------:|
| 命令字 | command       | byte                   | integer            |
| 数据块 | param         | string(base64 encoded) | string             |


#### 行驶记录数据上传 `"msg_id": 1792` 0x0700

| Field      | Json Key name | Value Type             | Value Type in Json |
|:----------:|:-------------:|:----------------------:|:------------------:|
| 应答流水号 | seq           | word                   | integer            |
| 命令字     | command       | byte                   | integer            |
| 数据块     | data          | string(base64 encoded) | string             |


#### 行驶记录参数下传命令 `"msg_id": 34561` 0x8701

| Field  | Json Key name | Value Type             | Value Type in Json |
|:------:|:-------------:|:----------------------:|:------------------:|
| 命令字 | command       | byte                   | integer            |
| 数据块 | param         | string(base64 encoded) | string             |


#### 电子运单上报 `"msg_id": 1793` 0x0701

| Field        | Json Key name | Value Type             | Value Type in Json |
|:------------:|:-------------:|:----------------------:|:------------------:|
| 电子运单长度 | length        | dword                  | integer            |
| 电子运单内容 | data          | string(base64 encoded) | string             |


#### 上报驾驶员身份信息请求 `"msg_id": 34562` 0x8702

空 Json


#### 驾驶员身份信息采集上报 `"msg_id": 1794` 0x0702

| Field          | Json Key name | Value Type | Value Type in Json |
|:--------------:|:-------------:|:----------:|:------------------:|
| 状态           | status        | byte       | integer            |
| 时间           | time          | string     | string             |
| IC 卡读取结果  | ic_result     | byte       | integer            |
| 驾驶员姓名     | driver_name   | string     | string             |
| 从业资格证编码 | certificate   | string     | string             |
| 发证机构名称   | organization  | string     | string             |
| 证件有效期     | cert_expiry   | string     | string             |


#### 定位数据批量上传 `"msg_id": 1796` 0x0704

| Field          | Json Key name | Value Type | Value Type in Json |
|:--------------:|:-------------:|:----------:|:------------------:|
| 位置数据类型   | type          | byte       | integer            |
| 数据项个数     | length        | word       | integer            |
| 位置汇报数据项 | location      | list       | list of location   |


#### CAN 总线数据上传 `"msg_id": 1797` 0x0705

| Field                | Json Key name | Value Type | Value Type in Json     |
|:--------------------:|:-------------:|:----------:|:----------------------:|
| 数据项个数           | length        | word       | integer                |
| CAN 总线数据接收时间 | time          | bcd(5)     | integer                |
| CAN 总线数据项       | can_data      | list       | list of can data.      |
| CAN 总线通道号       | channel       | 1 bit      | integer                |
| 帧类型               | frame_type    | 1 bit      | integer                |
| 数据采集方式         | data_method   | 1 bit      | integer                |
| CAN 总线 ID          | id            | 29 bits    | integer                |
| CAN 数据             | data          | binary     | string(base64 encoded) |


#### 多媒体时间信息上传 `"msg_id": 2048` 0x0800

| Field          | Json Key name | Value Type | Value Type in Json |
|:--------------:|:-------------:|:----------:|:------------------:|
| 多媒体数据 ID  | id            | dword      | integer            |
| 多媒体类型     | type          | byte       | integer            |
| 多媒体编码格式 | format        | byte       | integer            |
| 事件项编码     | event         | byte       | integer            |
| 通道 ID        | channel       | byte       | integer            |


#### 多媒体数据上传 `"msg_id": 2049` 0x0801

| Field          | Json Key name | Value Type | Value Type in Json     |
|:--------------:|:-------------:|:----------:|:----------------------:|
| 多媒体 ID      | id            | dword      | integer                |
| 多媒体类型     | type          | byte       | integer                |
| 多媒体编码格式 | format        | byte       | integer                |
| 事件项编码     | event         | byte       | integer                |
| 通道 ID        | channel       | byte       | integer                |
| 位置信息汇报   | location      | byte(28)   | map                    |
| 多媒体数据包   | multimedia    | binary     | string(base64 encoded) |



#### 多媒体数据上传应答 `"msg_id": 34816` 0x8800

| Field          | Json Key name | Value Type | Value Type in Json |
|:--------------:|:-------------:|:----------:|:------------------:|
| 多媒体 ID      | mm_id         | dword      | integer            |
| 重传包总数     | length        | byte       | integer            |
| 重传包 ID 列表 | retx_ids      | list       | list of retry IDs  |


#### 摄像头立即拍摄命令 `"msg_id": 34817` 0x8801

| Field             | Json Key name | Value Type | Value Type in Json |
|:-----------------:|:-------------:|:----------:|:------------------:|
| 通道 ID           | channel_id    | byte       | integer            |
| 拍摄命令          | command       | word       | integer            |
| 拍照间隔/录像时间 | period        | word       | integer            |
| 保存标志          | save          | byte       | integer            |
| 分辨率            | resolution    | byte       | integer            |
| 图像/视频质量     | quality       | byte       | integer            |
| 亮度              | bright        | byte       | integer            |
| 对比度            | contrast      | byte       | integer            |
| 饱和度            | saturate      | byte       | integer            |
| 色度              | chromaticity  | byte       | integer            |


#### 摄像头立即拍摄应答 `"msg_id": 2053` 0x0805

| Field          | Json Key name | Value Type     | Value Type in Json |
|:--------------:|:-------------:|:--------------:|:------------------:|
| 应答流水号     | seq           | word           | integer            |
| 结果           | result        | byte           | integer            |
| 多媒体 ID 个数 | length        | word           | integer            |
| 多媒体 ID 列表 | ids           | byte(4*length) | integer            |


#### 存储多媒体数据检索 `"msg_id": 34818` 0x8802

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 多媒体类型 |               | byte       |                    |
| 通道 ID    |               | byte       |                    |
| 事件项编码 |               | byte       |                    |
| 起始时间   |               | string     |                    |
| 结束时间   |               | string     |                    |


#### 存储多媒体数据检索应答 `"msg_id": 2050` 0x0802

| Field            | Json Key name | Value Type | Value Type in Json    |
|:----------------:|:-------------:|:----------:|:---------------------:|
| 应答流水号       | seq           | word       | integer               |
| 多媒体数据项总数 | length        | word       | integer               |
| 检索项           | result        | list       | list of search result |
| 多媒体 ID        | id            | dword      | integer               |
| 多媒体类型       | type          | byte       | integer               |
| 通道 ID          | channel       | byte       | integer               |
| 事件项编码       | event         | byte       | integer               |
| 位置信息汇报     | location      | byte(28)   | map                   |


#### 存储多媒体数据上传命令 `"msg_id": 34819` 0x8803

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 多媒体类型 | type          | byte       | integer            |
| 通道 ID    | channel       | byte       | integer            |
| 事件项编码 | event         | byte       | integer            |
| 起始时间   | start_time    | string     | string             |
| 结束时间   | end_time      | string     | string             |
| 删除标志   | delete        | byte       | integer            |


#### 录音开始命令 `"msg_id": 34820` 0x8804

| Field      | Json Key name | Value Type | Value Type in Json |
|:----------:|:-------------:|:----------:|:------------------:|
| 录音命令   | command       | byte       | integer            |
| 录音时间   | time          | word       | integer            |
| 保存标志   | save          | byte       | integer            |
| 音频采样率 | rate          | byte       | integer            |


#### 单条存储多媒体j叔叔检索上传命令 `"msg_id": 34821` 0x8805

| Field     | Json Key name | Value Type | Value Type in Json |
|:---------:|:-------------:|:----------:|:------------------:|
| 多媒体 ID | id            | dword      | integer            |
| 删除标志  | flag          | byte       | integer            |


#### 数据下行透传 `"msg_id": 35072` 0x8900

| Field        | Json Key name | Value Type | Value Type in Json     |
|:------------:|:-------------:|:----------:|:----------------------:|
| 透传消息类型 | type          | byte       | integer                |
| 透传消息内容 | data          | binary     | string(base64 encoded) |


#### 数据上行透传 `"msg_id": 2304` 0x0900

| Field        | Json Key name | Value Type | Value Type in Json     |
|:------------:|:-------------:|:----------:|:----------------------:|
| 透传消息类型 | type          | byte       | integer                |
| 透传消息内容 | data          | binary     | string(base64 encoded) |


#### 数据压缩上报 `"msg_id": 2305` 0x0901

| Field        | Json Key name | Value Type | Value Type in Json     |
|:------------:|:-------------:|:----------:|:----------------------:|
| 压缩消息长度 | length        | dword      | integer                |
| 压缩消息体   | data          | binary     | string(base64 encoded) |


#### 平台 RSA 公钥 `"msg_id": 35328` 0x8A00

| Field | Json Key name | Value Type | Value Type in Json     |
|:-----:|:-------------:|:----------:|:----------------------:|
| e     | e             | dword      | integer                |
| n     | n             | byte(128)  | string(base64 encoded) |


#### 终端 RSA 公钥 `"msg_id": 2560` 0x0A00

| Field | Json Key name | Value Type | Value Type in Json     |
|:-----:|:-------------:|:----------:|:----------------------:|
| e     | e             | dword      | integer                |
| n     | n             | byte(128)  | string(base64 encoded) |


#### 保留 0x8F00 ~ 0x8FFF

#### 保留 0x0F00 ~ 0x0FFF
