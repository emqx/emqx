# EMQX GBT/32960 网关

该文档定义了 **GBT/32960** 网关和 **EMQX** 之间数据交换的格式

约定:
- Payload 采用 Json 格式进行组装
- Json Key 采用大驼峰格式命名
- 使用车辆的 `vin` 值作为 `clientid`
- 默认挂载点为: gbt32960/${clientid}

# Upstream
数据流向: Terminal -> GBT/32960 -> EMQX

## 车辆登入
Topic: gbt32960/${clientid}/upstream/vlogin

```json
{
    "Cmd": 1,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "ICCID": "12345678901234567890",
        "Id": "C",
        "Length": 1,
        "Num": 1,
        "Seq": 1,
        "Time": {
            "Day": 29,
            "Hour": 12,
            "Minute": 19,
            "Month": 12,
            "Second": 20,
            "Year": 12
        }
    }
}
```

其中

| 字段      | 类型    | 描述                                                         |
| --------- | ------- | ------------------------------------------------------------ |
| `Cmd`     | Integer | 命令单元； `1` 表示车辆登入                                  |
| `Encrypt` | Integer | 数据单元加密方式，`1` 表示不加密，`2` 数据经过 RSA 加密，`3` 数据经过 ASE128 算法加密；`254` 表示异常；`255` 表示无效；其他预留 |
| `Vin`     | String  | 唯一识别码，即车辆 VIN 码                                    |
| `Data`    | Object  | 数据单元， JSON 对象格式。                                   |

车辆登入的数据单元格式为

| 字段     | 类型    | 描述                                                         |
| -------- | ------- | ------------------------------------------------------------ |
| `Time`   | Object  | 数据采集时间，按年，月，日，时，分，秒，格式见示例。         |
| `Seq`    | Integer | 登入流水号                                                   |
| `ICCID`  | String  | 长度为20的字符串，SIM 卡的 ICCID 号                          |
| `Num`    | Integer | 可充电储能子系统数，有效值 0 ~ 250                           |
| `Length` | Integer | 可充电储能系统编码长度，有效值 0 ~ 50                        |
| `Id`     | String  | 可充电储能系统编码，长度为 "子系统数" 与  "编码长度" 值的乘积 |

## 车辆登出

Topic: gbt32960/${clientid}/upstream/vlogout

车辆登出的 `Cmd` 值为 4，其余字段含义与登入相同：

```json
{
    "Cmd": 4,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Seq": 1,
        "Time": {
            "Day": 1,
            "Hour": 2,
            "Minute": 59,
            "Month": 1,
            "Second": 0,
            "Year": 16
        }
    }
}
```

## 实时信息上报

Topic: gbt32960/${clientid}/upstream/info

> 不同信息类型上报，格式上只有 Infos 里面的对象属性不同，通过 `Type` 进行区分
> Infos 为数组，代表车载终端每次报文可以上报多个信息

### 整车数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "AcceleratorPedal": 90,
                "BrakePedal": 0,
                "Charging": 1,
                "Current": 15000,
                "DC": 1,
                "Gear": 5,
                "Mileage": 999999,
                "Mode": 1,
                "Resistance": 6000,
                "SOC": 50,
                "Speed": 2000,
                "Status": 1,
                "Type": "Vehicle",
                "Voltage": 5000
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 2,
            "Minute": 59,
            "Month": 1,
            "Second": 0,
            "Year": 16
        }
    }
}
```



其中，整车信息字段含义如下：

| 字段         | 类型    | 描述                                                         |
| ------------ | ------- | ------------------------------------------------------------ |
| `Type`       | String  | 数据类型，`Vehicle` 表示该结构为整车信息                     |
| `Status`     | Integer | 车辆状态，`1` 表示启动状态；`2` 表示熄火；`3` 表示其状态；`254` 表示异常；`255` 表示无效 |
| `Charging`   | Integer | 充电状态，`1` 表示停车充电；`2` 行驶充电；`3` 未充电状态；`4` 充电完成；`254` 表示异常；`255` 表示无效 |
| `Mode`       | Integer | 运行模式，`1` 表示纯电；`2` 混动；`3` 燃油；`254` 表示异常；`255` 表示无效 |
| `Speed`      | Integer | 车速，有效值 （ 0~ 2200，表示 0 km/h ~ 220 km/h），单位 0.1 km/h |
| `Mileage`    | Integer | 累计里程，有效值  0 ~9,999,999（表示 0 km ~ 999,999.9 km），单位 0.1 km |
| `Voltage`    | Integer | 总电压，有效值范围 0 ~10000（表示 0 V ~  1000 V）单位 0.1 V  |
| `Current`    | Integer | 总电流，有效值 0 ~ 20000 （偏移量 1000，表示 -1000 A ~ +1000 A，单位 0.1 A |
| `SOC`        | Integer | SOC，有效值 0 ~ 100（表示 0% ~ 100%）                        |
| `DC`         | Integer | DC，`1` 工作；`2` 断开；`254` 表示异常；`255` 表示无效       |
| `Gear`       | Integer | 档位，参考原协议的 表 A.1，此值为其转换为整数的值            |
| `Resistance` | Integer | 绝缘电阻，有效范围 0 ~ 60000（表示 0 k欧姆 ~ 60000 k欧姆）   |

### 驱动电机数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "Motors": [
                    {
                        "CtrlTemp": 125,
                        "DCBusCurrent": 31203,
                        "InputVoltage": 30012,
                        "MotorTemp": 125,
                        "No": 1,
                        "Rotating": 30000,
                        "Status": 1,
                        "Torque": 25000
                    },
                    {
                        "CtrlTemp": 125,
                        "DCBusCurrent": 30200,
                        "InputVoltage": 32000,
                        "MotorTemp": 145,
                        "No": 2,
                        "Rotating": 30200,
                        "Status": 1,
                        "Torque": 25300
                    }
                ],
                "Number": 2,
                "Type": "DriveMotor"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 2,
            "Minute": 59,
            "Month": 1,
            "Second": 0,
            "Year": 16
        }
    }
}
```

其中，驱动电机数据各个字段的含义是

| 字段     | 类型    | 描述                           |
| -------- | ------- | ------------------------------ |
| `Type`   | String  | 数据类型，此处为  `DriveMotor` |
| `Number` | Integer | 驱动电机个数，有效值 1~253     |
| `Motors` | Array   | 驱动电机数据列表               |

驱动电机数据字段为：

| 字段           | 类型     | 描述                                                         |
| -------------- | -------- | ------------------------------------------------------------ |
| `No`           | Integer  | 驱动电机序号，有效值 1~253                                   |
| `Status`       | Integer  | 驱动电机状态，`1` 表示耗电；`2`发电；`3` 关闭状态；`4` 准备状态；`254` 表示异常；`255` 表示无效 |
| `CtrlTemp`     | Integer  | 驱动电机控制器温度，有效值 0~250（数值偏移 40°C，表示 -40°C ~ +210°C）单位 °C |
| `Rotating`     | Interger | 驱动电机转速，有效值 0~65531（数值偏移 20000表示 -20000 r/min ~ 45531 r/min）单位 1 r/min |
| `Torque`       | Integer  | 驱动电机转矩，有效值 0~65531（数据偏移量 20000，表示 - 2000 N·m ~ 4553.1 N·m）单位 0.1 N·m |
| `MotorTemp`    | Integer  | 驱动电机温度，有效值  0~250（数据偏移量 40 °C，表示 -40°C ~ +210°C）单位 1°C |
| `InputVoltage` | Integer  | 电机控制器输入电压，有效值 0~60000（表示 0V ~ 6000V）单位 0.1 V |
| `DCBusCurrent` | Interger | 电机控制器直流母线电流，有效值 0~20000（数值偏移 1000A，表示 -1000A ~ +1000 A）单位 0.1 A |

### 燃料电池数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "CellCurrent": 12000,
                "CellVoltage": 10000,
                "DCStatus": 1,
                "FuelConsumption": 45000,
                "H_ConcSensorCode": 11,
                "H_MaxConc": 35000,
                "H_MaxPress": 500,
                "H_MaxTemp": 12500,
                "H_PressSensorCode": 12,
                "H_TempProbeCode": 10,
                "ProbeNum": 2,
                "ProbeTemps": [120, 121],
                "Type": "FuelCell"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 2,
            "Minute": 59,
            "Month": 1,
            "Second": 0,
            "Year": 16
        }
    }
}
```

其中，燃料电池数据各个字段的含义是

| 字段                | 类型    | 描述                                                         |
| ------------------- | ------- | ------------------------------------------------------------ |
| `Type`              | String  | 数据类型，此处为 `FuleCell`                                  |
| `CellVoltage`       | Integer | 燃料电池电压，有效值范围 0~20000（表示 0V ~ 2000V）单位 0.1 V |
| `CellCurrent`       | Integer | 燃料电池电流，有效值范围 0~20000（表示 0A ~ +2000A）单位 0.1 A |
| `FuelConsumption`   | Integer | 燃料消耗率，有效值范围 0~60000（表示 0kg/100km ~ 600 kg/100km) 单位 0.01 kg/100km |
| `ProbeNum`          | Integer | 燃料电池探针总数，有效值范围 0~65531                         |
| `ProbeTemps`        | Array   | 燃料电池每探针温度值                                         |
| `H_MaxTemp`         | Integer | 氢系统最高温度，有效值 0~2400（偏移量40°C，表示 -40°C ~ 200°C）单位 0.1 °C |
| `H_TempProbeCode`   | Integer | 氢系统最高温度探针代号，有效值 1~252                         |
| `H_MaxConc`         | Integer | 氢气最高浓度，有效值 0~60000（表示 0mg/kg ~ 50000 mg/kg）单位 1mg/kg |
| `H_ConcSensorCode`  | Integer | 氢气最高浓度传感器代号，有效值 1~252                         |
| `H_MaxPress`        | Integer | 氢气最高压力，有效值 0~1000（表示 0 MPa ~ 100 MPa）最小单位 0.1 MPa |
| `H_PressSensorCode` | Integer | 氢气最高压力传感器代号，有效值 1~252                         |
| `DCStatus`          | Integer | 高压 DC/DC状态，`1` 表示工作；`2`断开                        |

### 发动机数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "CrankshaftSpeed": 2000,
                "FuelConsumption": 200,
                "Status": 1,
                "Type": "Engine"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 22,
            "Minute": 59,
            "Month": 10,
            "Second": 0,
            "Year": 16
        }
    }
}
```

其中，发动机数据各个字段的含义是

| 字段              | 类型    | 描述                                                         |
| ----------------- | ------- | ------------------------------------------------------------ |
| `Type`            | String  | 数据类型，此处为 `Engine`                                    |
| `Status`          | Integer | 发动机状态，`1` 表示启动；`2` 关闭                           |
| `CrankshaftSpeed` | Integer | 曲轴转速，有效值 0~60000（表示 0r/min ~ 60000r/min）单位 1r/min |
| `FuelConsumption` | Integer | 燃料消耗率，有效范围 0~60000（表示 0L/100km ~ 600L/100km）单位 0.01 L/100km |



### 车辆位置数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "Latitude": 100,
                "Longitude": 10,
                "Status": 0,
                "Type": "Location"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 22,
            "Minute": 59,
            "Month": 10,
            "Second": 0,
            "Year": 16
        }
    }
}
```

其中，车辆位置数据各个字段的含义是

| 字段        | 类型    | 描述                                                  |
| ----------- | ------- | ----------------------------------------------------- |
| `Type`      | String  | 数据类型，此处为 `Location`                           |
| `Status`    | Integer | 定位状态，见原协议表15，此处为所有比特位的整型值      |
| `Longitude` | Integer | 经度，以度为单位的纬度值乘以 10^6，精确到百万分之一度 |
| `Latitude`  | Integer | 纬度，以度为单位的纬度值乘以 10^6，精确到百万分之一度 |



### 极值数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "MaxBatteryVoltage": 7500,
                "MaxTemp": 120,
                "MaxTempProbeNo": 12,
                "MaxTempSubsysNo": 14,
                "MaxVoltageBatteryCode": 10,
                "MaxVoltageBatterySubsysNo": 12,
                "MinBatteryVoltage": 2000,
                "MinTemp": 40,
                "MinTempProbeNo": 13,
                "MinTempSubsysNo": 15,
                "MinVoltageBatteryCode": 11,
                "MinVoltageBatterySubsysNo": 13,
                "Type": "Extreme"
            }
        ],
        "Time": {
            "Day": 30,
            "Hour": 12,
            "Minute": 22,
            "Month": 5,
            "Second": 59,
            "Year": 17
        }
    }
}
```

其中，极值数据各个字段的含义是

| 字段                        | 类型    | 描述                                                         |
| --------------------------- | ------- | ------------------------------------------------------------ |
| `Type`                      | String  | 数据类型，此处为 `Extreme`                                   |
| `MaxVoltageBatterySubsysNo` | Integer | 最高电压电池子系统号，有效值 1~250                           |
| `MaxVoltageBatteryCode`     | Integer | 最高电压电池单体代号，有效值 1~250                           |
| `MaxBatteryVoltage`         | Integer | 电池单体电压最高值，有效值 0~15000（表示 0V~15V）单位 0.001V |
| `MinVoltageBatterySubsysNo` | Integer | 最低电压电池子系统号，有效值 1~250                           |
| `MinVoltageBatteryCode`     | Integer | 最低电压电池单体代号，有效值 1~250                           |
| `MinBatteryVoltage`         | Integer | 电池单体电压最低值，有效值 0~15000（表示 0V~15V）单位 0.001V |
| `MaxTempSubsysNo`           | Integer | 最高温度子系统号，有效值 1~250                               |
| `MaxTempProbeNo`            | Integer | 最高温度探针序号，有效值 1~250                               |
| `MaxTemp`                   | Integer | 最高温度值，有效值范围 0~250（偏移量40，表示 -40°C ~ +210°C）  |
| `MinTempSubsysNo`           | Integer | 最低温度子系统号，有效值 1~250                               |
| `MinTempProbeNo`            | Integer | 最低温度探针序号，有效值 1~250                               |
| `MinTemp`                   | Integer | 最低温度值，有效值范围 0~250（偏移量40，表示 -40°C ~ +210°C）  |



### 报警数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "FaultChargeableDeviceNum": 1,
                "FaultChargeableDeviceList": ["00C8"],
                "FaultDriveMotorNum": 0,
                "FaultDriveMotorList": [],
                "FaultEngineNum": 1,
                "FaultEngineList": ["006F"],
                "FaultOthersNum": 0,
                "FaultOthersList": [],
                "GeneralAlarmFlag": 3,
                "MaxAlarmLevel": 1,
                "Type": "Alarm"
            }
        ],
        "Time": {
            "Day": 20,
            "Hour": 22,
            "Minute": 23,
            "Month": 12,
            "Second": 59,
            "Year": 17
        }
    }
}
```

其中，报警数据各个字段的含义是

| 字段                        | 类型    | 描述                                                         |
| --------------------------- | ------- | ------------------------------------------------------------ |
| `Type`                      | String  | 数据类型，此处为 `Alarm`                                     |
| `MaxAlarmLevel`             | Integer | 最高报警等级，有效值范围 0~3，`0` 表示无故障，`1` 表示 `1` 级故障 |
| `GeneralAlarmFlag`          | Integer | 通用报警标志位，见原协议表 18                                |
| `FaultChargeableDeviceNum`  | Integer | 可充电储能装置故障总数，有效值 0~252                         |
| `FaultChargeableDeviceList` | Array   | 可充电储能装置故障代码列表                                   |
| `FaultDriveMotorNum`        | Integer | 驱动电机故障总数，有效置范围 0 ~252                          |
| `FaultDriveMotorList`       | Array   | 驱动电机故障代码列表                                         |
| `FaultEngineNum`            | Integer | 发动机故障总数，有效值范围 0~252                             |
| `FaultEngineList`           | Array   | 发动机故障代码列表                                           |
| `FaultOthersNum`            | Integer | 其他故障总数                                                 |
| `FaultOthersList`           | Array   | 其他故障代码列表                                             |



### 可充电储能装置电压数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "Number": 2,
                "SubSystems": [
                    {
                        "CellsTotal": 2,
                        "CellsVoltage": [5000],
                        "ChargeableCurrent": 10000,
                        "ChargeableSubsysNo": 1,
                        "ChargeableVoltage": 5000,
                        "FrameCellsCount": 1,
                        "FrameCellsIndex": 0
                    },
                    {
                        "CellsTotal": 2,
                        "CellsVoltage": [5001],
                        "ChargeableCurrent": 10001,
                        "ChargeableSubsysNo": 2,
                        "ChargeableVoltage": 5001,
                        "FrameCellsCount": 1,
                        "FrameCellsIndex": 1
                    }
                ],
                "Type": "ChargeableVoltage"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 22,
            "Minute": 59,
            "Month": 10,
            "Second": 0,
            "Year": 16
        }
    }
}
```



其中，字段定义如下

| 字段        | 类型    | 描述                                 |
| ----------- | ------- | ------------------------------------ |
| `Type`      | String  | 数据类型，此处为 `ChargeableVoltage` |
| `Number`    | Integer | 可充电储能子系统个数，有效范围 1~250 |
| `SubSystem` | Object  | 可充电储能子系统电压信息列表         |

可充电储能子系统电压信息数据格式：

| 字段                 | 类型    | 描述                                                         |
| -------------------- | ------- | ------------------------------------------------------------ |
| `ChargeableSubsysNo` | Integer | 可充电储能子系统号，有效值范围，1~250                        |
| `ChargeableVoltage`  | Integer | 可充电储能装置电压，有效值范围，0~10000（表示 0V ~ 1000V）单位 0.1 V |
| `ChargeableCurrent`  | Integer | 可充电储能装置电流，有效值范围，0~20000（数值偏移量 1000A，表示 -1000A ~ +1000A）单位 0.1 A |
| `CellsTotal`         | Integer | 单体电池总数，有效值范围 1~65531                             |
| `FrameCellsIndex`    | Integer | 本帧起始电池序号，当本帧单体个数超过 200 时，应该拆分多个帧进行传输，有效值范围 1~65531 |
| `FrameCellsCount`    | Integer | 本帧单体电池总数，有效值范围 1~200                           |
| `CellsVoltage`       | Array   | 单体电池电压，有效值范围 0~60000（表示 0V ~ 60.000V）单位 0.001V |



### 可充电储能装置温度数据

```json
{
    "Cmd": 2,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Infos": [
            {
                "Number": 2,
                "SubSystems": [
                    {
                        "ChargeableSubsysNo": 1,
                        "ProbeNum": 10,
                        "ProbesTemp": [0, 0, 0, 0, 0, 0, 0, 0, 19, 136]
                    },
                    {
                        "ChargeableSubsysNo": 2,
                        "ProbeNum": 1,
                        "ProbesTemp": [100]
                    }
                ],
                "Type": "ChargeableTemp"
            }
        ],
        "Time": {
            "Day": 1,
            "Hour": 22,
            "Minute": 59,
            "Month": 10,
            "Second": 0,
            "Year": 16
        }
    }
}
```
其中，数据格式为：

| 字段         | 类型    | 描述                              |
| ------------ | ------- | --------------------------------- |
| `Type`       | String  | 数据类型，此处为 `ChargeableTemp` |
| `Number`     | Integer | 可充电储能子系统温度信息列表长度  |
| `SubSystems` | Object  | 可充电储能子系统温度信息列表      |

可充电储能子系统温度信息格式为

| 字段                 | 类型     | 描述                                 |
| -------------------- | -------- | ------------------------------------ |
| `ChargeableSubsysNo` | Ineteger | 可充电储能子系统号，有效值 1~250     |
| `ProbeNum`           | Integer  | 可充电储能温度探针个数               |
| `ProbesTemp`         | Array    | 可充电储能子系统各温度探针温度值列表 |



## 数据补发

Topic: gbt32960/${clientid}/upstream/reinfo

**数据格式: 略** (与实时数据上报相同)

# Downstream

> 请求数据流向: EMQX -> GBT/32960 -> Terminal

> 应答数据流向: Terminal -> GBT/32960 -> EMQX

下行主题: gbt32960/${clientid}/dnstream
上行应答主题: gbt32960/${clientid}/upstream/response

## 参数查询



**Req:**

```json
{
    "Action": "Query",
    "Total": 2,
    "Ids": ["0x01", "0x02"]
}
```

| 字段     | 类型    | 描述                                               |
| -------- | ------- | -------------------------------------------------- |
| `Action` | String  | 下发命令类型，此处为 `Query`                       |
| `Total`  | Integer | 查询参数总数                                       |
| `Ids`    | Array   | 需查询参数的 ID 列表，具体 ID 含义见原协议 表 B.10 |

**Response:**

```json
{
    "Cmd": 128,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Total": 2,
        "Params": [
            {"0x01": 6000},
            {"0x02": 10}
        ],
        "Time": {
            "Day": 2,
            "Hour": 11,
            "Minute": 12,
            "Month": 2,
            "Second": 12,
            "Year": 17
        }
    }
}
```



## 参数设置

**Req:**
```json
{
    "Action": "Setting",
    "Total": 2,
    "Params": [{"0x01": 5000},
               {"0x02": 200}]
}
```

| 字段     | 类型    | 描述                           |
| -------- | ------- | ------------------------------ |
| `Action` | String  | 下发命令类型，此处为 `Setting` |
| `Total`  | Integer | 设置参数总数                   |
| `Params` | Array   | 需设置参数的 ID 和 值          |

**Response:**

```json
// fixme? 终端是按照这种方式返回?
{
    "Cmd": 129,
    "Encrypt": 1,
    "Vin": "1G1BL52P7TR115520",
    "Data": {
        "Total": 2,
        "Params": [
            {"0x01": 5000},
            {"0x02": 200}
        ],
        "Time": {
            "Day": 2,
            "Hour": 11,
            "Minute": 12,
            "Month": 2,
            "Second": 12,
            "Year": 17
        }
    }
}
```

## 终端控制
**命令的不同, 参数不同; 无参数时为空**

远程升级:
**Req:**

```json
{
    "Action": "Control",
    "Command": "0x01",
    "Param": {
        "DialingName": "hz203",
        "Username": "user001",
        "Password": "password01",
        "Ip": "192.168.199.1",
        "Port": 8080,
        "ManufacturerId": "BMWA",
        "HardwareVer": "1.0.0",
        "SoftwareVer": "1.0.0",
        "UpgradeUrl": "ftp://emqtt.io/ftp/server",
        "Timeout": 10
    }
}
```

| 字段      | 类型    | 描述                           |
| --------- | ------- | ------------------------------ |
| `Action`  | String  | 下发命令类型，此处为 `Control` |
| `Command` | Integer | 下发指令 ID,见原协议表 B.15    |
| `Param`   | Object  | 命令参数                       |

列表

车载终端关机:

```json
{
    "Action": "Control",
    "Command": "0x02"
}
```

...

车载终端报警:
```json
{
    "Action": "Control",
    "Command": "0x06",
    "Param": {"Level": 0, "Message": "alarm message"}
}
```
