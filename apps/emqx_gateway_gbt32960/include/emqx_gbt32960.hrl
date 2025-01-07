%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-record(frame, {cmd, ack, vin, encrypt, length, data, check, rawdata}).

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
        C == ?ACK_VIN_REPEAT)
).

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
% 0x09~0x7F: Reserved by upstream system
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
-define(ACK_IS_CMD, 16#FE).

%%--------------------------------------------------------------------
%% Encrypt Feilds
%%--------------------------------------------------------------------
-define(ENCRYPT_NONE, 16#01).
-define(ENCRYPT_RSA, 16#02).
-define(ENCRYPT_AES128, 16#03).
-define(ENCRYPT_ABNORMAL, 16#FE).
-define(ENCRYPT_INVAILD, 16#FF).

%%--------------------------------------------------------------------
%% Info Type Flags
%%--------------------------------------------------------------------
-define(INFO_TYPE_VEHICLE, 16#01).
-define(INFO_TYPE_DRIVE_MOTOR, 16#02).
-define(INFO_TYPE_FUEL_CELL, 16#03).
-define(INFO_TYPE_ENGINE, 16#04).
-define(INFO_TYPE_LOCATION, 16#05).
-define(INFO_TYPE_EXTREME, 16#06).
-define(INFO_TYPE_ALARM, 16#07).
-define(INFO_TYPE_CHARGEABLE_VOLTAGE, 16#08).
-define(INFO_TYPE_CHARGEABLE_TEMP, 16#09).
% 0x0A~0x2F: Customized data for Platform Exchange Protocol
% 0x30~0x7F: Reserved
% 0x80~0xFE: Customized by user

-define(DEFAULT_MOUNTPOINT, <<"gbt32960/${clientid}/">>).
-define(DEFAULT_DOWNLINK_TOPIC, <<"dnstream">>).
