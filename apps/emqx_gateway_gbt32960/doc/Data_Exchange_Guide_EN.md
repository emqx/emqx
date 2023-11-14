# EMQX GBT/32960 Gateway

This document defines the format of the data exchange internal the **GBT/32960** gateway and the **EMQX**.

Conventions:
- Payloads are assembled in Json format
- Json Keys are named in big hump format
- Use the `vin` value of the vehicle as the `clientid`.
- The default mountpoint is: `gbt32960/${clientid}`

# Upstream
Data flow: Terminal -> GBT/32960 Gateway -> EMQX

## Vehicle Login
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

definition:

| Field     | Type    | Description                                                                                                                  |
| --------- | ------- | ------------------------------------------------------------                                                                 |
| `Cmd`     | Integer | Command, `1` :: vehicle login                                                                                                |
| `Encrypt` | Integer | Data encryption method, `1` :: no encryption; `2` :: RSA; `3` :: ASE128; `254` :: abnormal; `255` :: invalid; other reserved |
| `Vin`     | String  | Unique identifier，namely, the vehicle VIN code                                                                               |
| `Data`    | Object  | The JSON format data                                                                                                         |

The data unit format for vehicle login is:

| Field    | Type    | Description                                                                                            |
| -------- | ------- | ------------------------------------------------------------                                           |
| `Time`   | Object  | Data collection time, in year, month, day, hour, minute, second. See the example for the format        |
| `Seq`    | Integer | Login sequence number                                                                                  |
| `ICCID`  | String  | String of length 20, the ICCID number of the SIM card                                                  |
| `Num`    | Integer | Number of rechargeable energy storage subsystems, valid values 0 ~ 250                                 |
| `Length` | Integer | Rechargeable energy storage system encoding length, valid value 0 ~ 50                                 |
| `Id`     | String  | Rechargeable energy storage system code, the length is the product of the `Num` and the `Length` value |

## Vehicle logout

Topic: gbt32960/${clientid}/upstream/vlogout

The `Cmd` value of vehicle logout is 4, and the meaning of the other fields is the same as that of the login.

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

## Real-time information reporting

Topic: gbt32960/${clientid}/upstream/info

> When reporting messages of different information types, only the object attribute is different, which are distinguished by the `Type` field.
> Infos is an array, which means that the vehicle terminal can report multiple information in each message

### Vehicle data

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



The meaning of the vehicle information field is as follows:

| Field        | Type    | Description                                                                                                                                                       |
| ------------ | ------- | ------------------------------------------------------------                                                                                                      |
| `Type`       | String  | Data type, `Vehicle` :: this is the vehicle information                                                                                                 |
| `Status`     | Integer | Vehicle status, `1` :: started; `2` :: stalled; `3` :: others; `254` :: abnormal; `255` :: invalid                                                                |
| `Charging`   | Integer | Charging status, `1` :: parking and charging; `2` :: driving and charging; `3` :: not charging; `4` :: charging is completed; `254` :: abnormal; `255` :: invalid |
| `Mode`       | Integer | Operating mode, `1` :: pure electric; `2` :: hybrid; `3` :: fuel; `254` :: abnormal; `255` :: invalid                                                             |
| `Speed`      | Integer | Vehicle speed, valid value (0~ 2200, indicating 0 km/h ~ 220 km/h), unit 0.1 km/h                                                                                 |
| `Mileage`    | Integer | Accumulated mileage, valid value 0 ~9,999,999 (representing 0 km ~ 999,999.9 km), unit 0.1 km                                                                     |
| `Voltage`    | Integer | Total voltage, valid value 0 ~10000 (representing 0 V ~ 1000 V) unit 0.1 V                                                                                        |
| `Current`    | Integer | Total current, valid value 0 ~ 20000 (offset 1000, :: -1000 A ~ +1000 A) unit 0.1 A                                                                               |
| `SOC`        | Integer | SOC, valid values 0 ~ 100 (representing 0% ~ 100%)                                                                                                                |
| `DC`         | Integer | DC, `1` works; `2` disconnects; `254` :: abnormal; `255` :: invalid                                                                                               |
| `Gear`       | Integer | Gear, refer to Table A.1 of the original protocol, this value is converted into an integer                                                                        |
| `Resistance` | Integer | Insulation resistance, valid range 0 ~ 60000 (representing 0 k ohm ~ 60000 k ohm)                                                                                 |



### Drive motor data

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

The meaning of each field of the drive motor data is:

| Field    | Type    | Description                               |
| -------- | ------- | -------------------------------           |
| `Type`   | String  | Data type, here is `DriveMotor`           |
| `Number` | Integer | Number of drive motors, valid value 1~253 |
| `Motors` | Array   | Drive motor data list                     |

The drive motor data fields are:

| Field          | Type     | Description                                                                                                         |
| -------------- | -------- | --------------------------------------------------------------------------------------------------------------------|
| `No`           | Integer  | Drive motor serial number, valid value 1 ~ 253                                                                      |
| `Status`       | Integer  | Drive motor status, `1` :: consuming; `2` producing; `3` closed; `4` ready; `254` :: abnormal; `255` :: invalid     |
| `CtrlTemp`     | Integer  | Drive motor controller temperature, valid value 0 ~ 250 (value offset 40°C, indicating -40°C ~ +210°C) unit °C      |
| `Rotating`     | Interger | Drive motor speed, valid value 0 ~ 65531 (numeric offset 20000 :: -20000 r/min ~ 45531 r/min) unit 1 r/min          |
| `Torque`       | Integer  | Drive motor torque, valid value 0 ~ 65531 (data offset 20000, represents - 2000 N·m ~ 4553.1 N·m) unit 0.1 N·m      |
| `MotorTemp`    | Integer  | Drive motor temperature, valid value 0 ~ 250 (data offset 40 °C, represents -40°C ~ +210°C) unit 1°C                |
| `InputVoltage` | Integer  | Motor controller input voltage, valid value 0 ~ 60000 (representing 0V ~ 6000V) unit 0.1 V                          |
| `DCBusCurrent` | Interger | Motor controller DC bus current, valid value 0 ~ 20000 (value offset 1000A, indicating -1000A ~ +1000 A) unit 0.1 A |


### Fuel cell data

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

The meaning of each field of fuel cell data is

| Field               | Type    | Description                                                                                                          |
| ------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------|
| `Type`              | String  | Data type, here is `FuleCell`                                                                                        |
| `CellVoltage`       | Integer | Fuel cell voltage, valid value range 0~20000 (representing 0V ~ 2000V) unit 0.1 V                                    |
| `CellCurrent`       | Integer | Fuel cell current, valid value range 0~20000 (representing 0A ~ +2000A) unit 0.1 A                                   |
| `FuelConsumption`   | Integer | Fuel consumption rate, valid value range 0~60000 (representing 0kg/100km ~ 600 kg/100km) unit 0.01 kg/100km          |
| `ProbeNum`          | Integer | Total number of fuel cell probes, valid value range 0~65531                                                          |
| `ProbeTemps`        | Array   | Fuel cell temperature value per probe                                                                                |
| `H_MaxTemp`         | Integer | Maximum temperature of the hydrogen system, effective value 0~2400 (offset 40°C, indicating -40°C ~ 200°C) unit 0.1 °C |
| `H_TempProbeCode`   | Integer | Hydrogen system maximum temperature probe code, valid value 1~252                                                    |
| `H_MaxConc`         | Integer | Maximum hydrogen concentration, valid value 0~60000 (representing 0mg/kg ~ 50000 mg/kg) unit 1mg/kg                  |
| `H_ConcSensorCode`  | Integer | Hydrogen maximum concentration sensor code, valid value 1~252                                                        |
| `H_MaxPress`        | Integer | Maximum pressure of hydrogen, valid value 0~1000 (representing 0 MPa ~ 100 MPa) minimum unit 0.1 MPa                 |
| `H_PressSensorCode` | Integer | Hydrogen maximum pressure sensor code, valid value 1~252                                                             |
| `DCStatus`          | Integer | High voltage DC/DC status, `1` :: working; `2` :: disconnected                                                       |

### Engine data

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

The meaning of each field of the engine data is

| Field              | Type    | Description                                                                                     |
| ------------------ | ------- | ------------------------------------------------------------------------------------------------|
| `Type`             | String  | Data type, here is `Engine`                                                                     |
| `Status`           | Integer | Engine status, `1` :: started; `2` :: shutdown                                                  |
| `CrankshaftSpeed`  | Integer | Crankshaft speed, valid value 0~60000 (representing 0r/min ~ 60000r/min) unit 1r/min              |
| `FuelConsumption`  | Integer | Fuel consumption rate, valid range 0~60000 (representing 0L/100km ~ 600L/100km) unit 0.01 L/100km |



### Vehicle location data

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

The meaning of each field of the vehicle location data is:

| Field       | Type    | Description                                                                                     |
| ----------- | ------- | ------------------------------------------------------------------------------------------------|
| `Type`      | String  | Data type, here is `Location`                                                                   |
| `Status`    | Integer | Positioning status, see table 15 of original protocol, here is the integer value of all bits    |
| `Longitude` | Integer | Longitude, latitude value in degrees multiplied by 10^6 to the nearest millionth of a degree    |
| `Latitude`  | Integer | Latitude, the latitude value in degrees multiplied by 10^6 to the nearest millionth of a degree |



### Maximum value data

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

Among them, the meaning of each field of extreme value data is

| Field                       | Type    | Description                                                                                      |
| --------------------------- | ------- | -------------------------------------------------------------------------------------------------|
| `Type`                      | String  | Data type, here is `Extreme`                                                                     |
| `MaxVoltageBatterySubsysNo` | Integer | Maximum voltage battery subsystem number, valid value 1~250                                      |
| `MaxVoltageBatteryCode`     | Integer | Maximum voltage battery cell code, valid value 1~250                                             |
| `MaxBatteryVoltage`         | Integer | Maximum value of the battery cell voltage, valid value 0~15000 (representing 0V ~ 15V) unit 0.001V |
| `MinVoltageBatterySubsysNo` | Integer | Minimum voltage battery subsystem number, valid value 1~250                                      |
| `MinVoltageBatteryCode`     | Integer | Minimum voltage battery cell code, valid value 1~250                                             |
| `MinBatteryVoltage`         | Integer | Minimum value of battery cell voltage, valid value 0~15000 (representing 0V ~ 15V) unit 0.001V     |
| `MaxTempSubsysNo`           | Integer | Maximum temperature subsystem number, valid value 1~250                                          |
| `MaxTempProbeNo`            | Integer | Maximum temperature probe serial number, valid value 1~250                                       |
| `MaxTemp`                   | Integer | Maximum temperature value, valid value range 0~250 (offset 40, representing -40°C ~ +210°C)        |
| `MinTempSubsysNo`           | Integer | Minimum temperature subsystem number, valid value 1~250                                          |
| `MinTempProbeNo`            | Integer | Minimum temperature probe serial number, valid value 1~250                                       |
| `MinTemp`                   | Integer | Minimum temperature value, valid value range 0~250 (offset 40, representing -40°C ~ +210°C)        |


### Alarm data

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

The meaning of each field of the alarm data is:

| Field                       | Type    | Description                                                                                |
| --------------------------- | ------- | -------------------------------------------------------------------------------------------|
| `Type`                      | String  | Data type, here is `Alarm`                                                                 |
| `MaxAlarmLevel`             | Integer | The maximum alarm level, valid value range is 0~3, `0` :: no fault, `1` :: `1` level fault |
| `GeneralAlarmFlag`          | Integer | General alarm flag, see original protocol table 18                                         |
| `FaultChargeableDeviceNum`  | Integer | Total number of rechargeable energy storage device faults, valid value 0~252               |
| `FaultChargeableDeviceList` | Array   | Rechargeable energy storage device fault code list                                         |
| `FaultDriveMotorNum`        | Integer | Total number of drive motor faults, valid setting range 0 ~252                             |
| `FaultDriveMotorList`       | Array   | Drive motor fault code list                                                                |
| `FaultEngineNum`            | Integer | Total number of engine faults, valid value range 0~252                                     |
| `FaultEngineList`           | Array   | Engine fault code list                                                                     |
| `FaultOthersNum`            | Integer | Total number of other faults                                                               |
| `FaultOthersList`           | Array   | Other fault code list                                                                      |



### Rechargeable energy storage device voltage data

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



The fields are defined as follows:

| Field       | Type    | Description                                                         |
| ----------- | ------- | --------------------------------------------------------------------|
| `Type`      | String  | Data type, here is `ChargeableVoltage`                              |
| `Number`    | Integer | Number of rechargeable energy storage subsystems, valid range 1~250 |
| `SubSystem` | Object  | Rechargeable energy storage subsystem voltage information list      |

Rechargeable energy storage subsystem voltage information data format:

| Field                | Type    | Description                                                                                                                                                            |
| -------------------- | ------- | -----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ChargeableSubsysNo` | Integer | Rechargeable energy storage subsystem number, valid value range, 1~250                                                                                                 |
| `ChargeableVoltage`  | Integer | Rechargeable energy storage device voltage, valid value range, 0~10000 (representing 0V ~ 1000V) unit 0.1 V                                                              |
| `ChargeableCurrent`  | Integer | Rechargeable energy storage device current, valid value range, 0~20000 (value offset 1000A, indicating -1000A ~ +1000A) unit 0.1 A                                       |
| `CellsTotal`         | Integer | Total number of cells, valid value range 1~65531                                                                                                                       |
| `FrameCellsIndex`    | Integer | The serial number of the cell at the beginning of this frame, when the number of single cells in this frame exceeds 200, it should be split into multiple frames for transmission, valid value range 1~65531 |
| `FrameCellsCount`    | Integer | The total number of cells in this frame, valid value range 1~200                                                                                                       |
| `CellsVoltage`       | Array   | Cells voltage, valid value range 0~60000 (representing 0V ~ 60.000V) unit 0.001V                                                                                         |



### Rechargeable energy storage device temperature data

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
The data format is:

| Field        | Type    | Description                                                               |
| ------------ | ------- | --------------------------------------------------------------------------|
| `Type`       | String  | Data type, here is `ChargeableTemp`                                       |
| `Number`     | Integer | Rechargeable energy storage subsystem temperature information list length |
| `SubSystems` | Object  | Rechargeable energy storage subsystem temperature information list        |

The rechargeable energy storage subsystem temperature information format is:

| Field                | Type     | Description                                                                                       |
| -------------------- | -------- | --------------------------------------------------------------------------------------------------|
| `ChargeableSubsysNo` | Ineteger | Rechargeable energy storage subsystem number, valid value 1~250                                   |
| `ProbeNum`           | Integer  | Number of rechargeable energy storage temperature probes                                          |
| `ProbesTemp`         | Array    | List of temperature values of each temperature probe of the rechargeable energy storage subsystem |



## Data reissue

Topic: gbt32960/${clientid}/upstream/reinfo

**Data format: omitted** (same as real-time data reporting)

# Downstream

> Request data flow direction: EMQX -> GBT/32960 Gateway -> Terminal

> Response data flow: Terminal -> GBT/32960 Gateway -> EMQX

Downstream topic: gbt32960/${clientid}/dnstream
Upstream response topic: gbt32960/${clientid}/upstream/response

## Parameters query



**Req:**

```json
{
    "Action": "Query",
    "Total": 2,
    "Ids": ["0x01", "0x02"]
}
```

| Field    | Type    | Description                                                                                         |
| -------- | ------- | ----------------------------------------------------------------------------------------------------|
| `Action` | String  | The type of downstream command, here is `Query`                                                     |
| `Total`  | Integer | Total number of query parameters                                                                    |
| `Ids`    | Array   | List of IDs that need to be queried. For specific ID meanings, see the original protocol Table B.10 |

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



## Parameters setting

**Req:**
```json
{
    "Action": "Setting",
    "Total": 2,
    "Params": [{"0x01": 5000},
               {"0x02": 200}]
}
```

| Field    | Type    | Description                                   |
| -------- | ------- | ----------------------------------------------|
| `Action` | String  | Type of downstream command, here is `Setting` |
| `Total`  | Integer | Set the total number of parameters            |
| `Params` | Array   | ID and value of parameters to be set          |

**Response:**

```json
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

## Terminal control
**Different commands have different parameters; if there are no parameters, it will be empty**

Remote Upgrade:
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

| Field     | Type    | Description                                         |
| --------- | ------- | ----------------------------------------------------|
| `Action`  | String  | Type of command issued, here is `Control`           |
| `Command` | Integer | Issued command ID, see original protocol table B.15 |
| `Param`   | Object  | Command parameters                                  |

The example list:

Shut down the vehicle terminal:

```json
{
    "Action": "Control",
    "Command": "0x02"
}
```

...

Vehicle terminal alarm:
```json
{
    "Action": "Control",
    "Command": "0x06",
    "Param": {"Level": 0, "Message": "alarm message"}
}
```
