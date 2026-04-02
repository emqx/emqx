%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-record(frame, {cmd, ack, vin, encrypt, length, data, check, rawdata, proto_ver = <<"2016">>}).

-type frame() :: #frame{}.

-define(CMD(CmdType), #frame{
    cmd = CmdType,
    ack = ?ACK_IS_CMD
}).

-define(CMD(CmdType, Data), #frame{
    cmd = CmdType,
    data = Data,
    ack = ?ACK_IS_CMD
}).

-define(IS_ACK_CODE(C),
    (C == ?ACK_SUCCESS orelse
        C == ?ACK_ERROR orelse
        C == ?ACK_VIN_REPEAT orelse
        C == ?ACK_VIN_NOT_EXIST orelse
        C == ?ACK_SIG_ERROR orelse
        C == ?ACK_STRUCT_ERROR orelse
        C == ?ACK_DECRYPT_ERROR)
).

%%--------------------------------------------------------------------
%% Protocol Version
%%--------------------------------------------------------------------
-define(PROTO_VER_2016, <<"2016">>).
-define(PROTO_VER_2025, <<"2025">>).

-define(IS_PROTO_VER_2016, #frame{proto_ver = ?PROTO_VER_2016}).
-define(IS_PROTO_VER_2025, #frame{proto_ver = ?PROTO_VER_2025}).

%%--------------------------------------------------------------------
%% CMD Feilds
%%--------------------------------------------------------------------
-define(CMD_VIHECLE_LOGIN, 16#01).
-define(CMD_INFO_REPORT, 16#02).
-define(CMD_INFO_RE_REPORT, 16#03).
-define(CMD_VIHECLE_LOGOUT, 16#04).
-define(CMD_PLATFORM_LOGIN, 16#05).
-define(CMD_PLATFORM_LOGOUT, 16#06).
-define(CMD_HEARTBEAT, 16#07).
-define(CMD_SCHOOL_TIME, 16#08).
-define(CMD_ACTIVATION, 16#09).
-define(CMD_ACTIVATION_RES, 16#0A).
-define(CMD_KEY_EXCHANGE, 16#0B).
% 0x0C~0x7F: Reserved by upstream system
% 0x80~0x82: Reserved by terminal data
-define(CMD_PARAM_QUERY, 16#80).
-define(CMD_PARAM_SETTING, 16#81).
-define(CMD_TERMINAL_CTRL, 16#82).

% 0x83~0xBF: Reserved by downstream system
% 0xC0~0xFE: Customized data for Platform Exchange Protocol

%%--------------------------------------------------------------------
%% ACK Feilds
%%--------------------------------------------------------------------
-define(ACK_SUCCESS, 16#01).
-define(ACK_ERROR, 16#02).
-define(ACK_VIN_REPEAT, 16#03).
-define(ACK_VIN_NOT_EXIST, 16#04).
-define(ACK_SIG_ERROR, 16#05).
-define(ACK_STRUCT_ERROR, 16#06).
-define(ACK_DECRYPT_ERROR, 16#07).
-define(ACK_IS_CMD, 16#FE).

%%--------------------------------------------------------------------
%% Encrypt Feilds
%%--------------------------------------------------------------------
-define(ENCRYPT_NONE, 16#01).
-define(ENCRYPT_RSA, 16#02).
-define(ENCRYPT_AES128, 16#03).
-define(ENCRYPT_SM2, 16#04).
-define(ENCRYPT_SM4, 16#05).
-define(ENCRYPT_ABNORMAL, 16#FE).
-define(ENCRYPT_INVAILD, 16#FF).

%%--------------------------------------------------------------------
%% Info Type Flags — GBT32960-2016
%%--------------------------------------------------------------------
-define(INFO_TYPE_VEHICLE, 16#01).
-define(INFO_TYPE_DRIVE_MOTOR, 16#02).
-define(INFO_TYPE_FUEL_CELL, 16#03).
-define(INFO_TYPE_ENGINE, 16#04).
-define(INFO_TYPE_LOCATION, 16#05).

-define(INFO_TYPE_2016_VEHICLE, ?INFO_TYPE_VEHICLE).
-define(INFO_TYPE_2016_DRIVE_MOTOR, ?INFO_TYPE_DRIVE_MOTOR).
-define(INFO_TYPE_2016_FUEL_CELL, ?INFO_TYPE_FUEL_CELL).
-define(INFO_TYPE_2016_ENGINE, ?INFO_TYPE_ENGINE).
-define(INFO_TYPE_2016_LOCATION, ?INFO_TYPE_LOCATION).
-define(INFO_TYPE_2016_EXTREME, 16#06).
-define(INFO_TYPE_2016_ALARM, 16#07).
-define(INFO_TYPE_2016_CHARGEABLE_BAT_VOLTAGE, 16#08).
-define(INFO_TYPE_2016_CHARGEABLE_BAT_TEMP, 16#09).
% 0x0A~0x2F: Customized data for Platform Exchange Protocol
% 0x30~0x7F: Reserved
% 0x80~0xFE: Customized by user

%% Keep old names for backward compatibility (used by 2016-only code paths)
-define(INFO_TYPE_EXTREME, ?INFO_TYPE_2016_EXTREME).
-define(INFO_TYPE_ALARM, ?INFO_TYPE_2016_ALARM).
-define(INFO_TYPE_CHARGEABLE_VOLTAGE, ?INFO_TYPE_2016_CHARGEABLE_BAT_VOLTAGE).
-define(INFO_TYPE_CHARGEABLE_TEMP, ?INFO_TYPE_2016_CHARGEABLE_BAT_TEMP).

%%--------------------------------------------------------------------
%% Info Type Flags — GBT32960-2025
%%--------------------------------------------------------------------
-define(INFO_TYPE_2025_VEHICLE, ?INFO_TYPE_VEHICLE).
-define(INFO_TYPE_2025_DRIVE_MOTOR, ?INFO_TYPE_DRIVE_MOTOR).
-define(INFO_TYPE_2025_FUEL_CELL, ?INFO_TYPE_FUEL_CELL).
-define(INFO_TYPE_2025_ENGINE, ?INFO_TYPE_ENGINE).
-define(INFO_TYPE_2025_LOCATION, ?INFO_TYPE_LOCATION).
-define(INFO_TYPE_2025_ALARM, 16#06).
-define(INFO_TYPE_2025_POWER_BATTERY_VOLTAGE, 16#07).
-define(INFO_TYPE_2025_POWER_BATTERY_TEMP, 16#08).
%% 0x09 - 0x2F: Customized data for Platform Exchange Protocol
-define(INFO_TYPE_2025_FUEL_CELL_STACK, 16#30).
-define(INFO_TYPE_2025_SUPER_CAPACITOR, 16#31).
-define(INFO_TYPE_2025_SUPER_CAPACITOR_EXTREME, 16#32).
-define(INFO_TYPE_2025_SIGNATURE, 16#FF).

-define(DEFAULT_MOUNTPOINT, <<"gbt32960/${clientid}/">>).
-define(DEFAULT_DOWNLINK_TOPIC, <<"dnstream">>).
