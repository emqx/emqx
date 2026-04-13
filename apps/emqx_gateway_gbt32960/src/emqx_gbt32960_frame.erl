%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gbt32960_frame).

-behaviour(emqx_gateway_frame).

-include("emqx_gbt32960.hrl").
-include_lib("emqx/include/logger.hrl").

%% emqx_gateway_frame callbacks
-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

% -define(FLAG, 1 / binary).
-define(BYTE, 8 / big - integer).
-define(WORD, 16 / big - integer).
-define(DWORD, 32 / big - integer).
%% CMD: 1, ACK: 1, VIN: 17, Enc: 1, Len: 2
-define(HEADER_SIZE, 22).

-define(IS_RESPONSE(Ack),
    Ack == ?ACK_SUCCESS orelse
        Ack == ?ACK_ERROR orelse
        Ack == ?ACK_VIN_REPEAT
).

-type phase() :: search_heading | parse.

-type parser_state() :: #{
    data := binary(),
    phase := phase(),
    proto_ver := binary()
}.

-ifdef(TEST).
-export([serialize/1]).
-endif.

%%--------------------------------------------------------------------
%% Init a Parser
%%--------------------------------------------------------------------

-spec initial_parse_state(map()) -> parser_state().
initial_parse_state(_) ->
    #{data => <<>>, phase => search_heading, proto_ver => ?PROTO_VER_2016}.

-spec serialize_opts() -> emqx_gateway_frame:serialize_options().
serialize_opts() ->
    #{}.

%%--------------------------------------------------------------------
%% Parse Message
%%--------------------------------------------------------------------
parse(Bin, State) ->
    case enter_parse(Bin, State) of
        {ok, Message, Rest} ->
            {ok, Message, Rest, State#{
                data => <<>>, phase => search_heading, proto_ver => ?PROTO_VER_2016
            }};
        {error, Error} ->
            {error, Error};
        {more_data_follow, {Partial, Version}} ->
            {more, State#{data => Partial, phase => parse, proto_ver => Version}}
    end.

enter_parse(Bin, #{phase := search_heading}) ->
    case search_heading(Bin) of
        {ok, Rest, Version} ->
            parse_msg(Rest, Version);
        Error ->
            Error
    end;
enter_parse(Bin, #{data := Data, proto_ver := Version}) ->
    parse_msg(<<Data/binary, Bin/binary>>, Version).

search_heading(<<16#23, 16#23, Rest/binary>>) ->
    {ok, Rest, ?PROTO_VER_2016};
search_heading(<<16#24, 16#24, Rest/binary>>) ->
    {ok, Rest, ?PROTO_VER_2025};
search_heading(<<_, Rest/binary>>) ->
    search_heading(Rest);
search_heading(<<>>) ->
    {error, invalid_frame}.

parse_msg(Binary, Version) ->
    case byte_size(Binary) >= ?HEADER_SIZE of
        true ->
            {Frame, Rest2} = parse_header(Binary),
            Frame1 = Frame#frame{proto_ver = Version},
            case byte_size(Rest2) >= Frame1#frame.length + 1 of
                true -> parse_body(Rest2, Frame1);
                false -> {more_data_follow, {Binary, Version}}
            end;
        false ->
            {more_data_follow, {Binary, Version}}
    end.

parse_header(<<Cmd, Ack, VIN:17/binary, Encrypt, Length:?WORD, Rest2/binary>> = Binary) ->
    Check = cal_check(Binary, ?HEADER_SIZE, undefined),
    {
        #frame{cmd = Cmd, ack = Ack, vin = VIN, encrypt = Encrypt, length = Length, check = Check},
        Rest2
    }.

parse_body(Binary, Frame = #frame{length = Length, check = OldCheck, encrypt = Encrypt}) ->
    <<Data:Length/binary, CheckByte, Rest/binary>> = Binary,
    Check = cal_check(Binary, Length, OldCheck),
    case CheckByte == Check of
        true ->
            RawData = decipher(Data, Encrypt),
            {ok, Frame#frame{data = parse_data(Frame, RawData), rawdata = RawData}, Rest};
        false ->
            {error, frame_check_error}
    end.

% Algo: ?ENCRYPT_NONE, ENCRYPT_RSA, ENCRYPT_AES128
decipher(Data, _Algo) ->
    % TODO: decypher data
    Data.

% Algo: ?ENCRYPT_NONE, ENCRYPT_RSA, ENCRYPT_AES128
encipher(Data, _Algo) ->
    % TODO: encipher data
    Data.

parse_data(
    #frame{cmd = ?CMD_VIHECLE_LOGIN, proto_ver = ?PROTO_VER_2016},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Seq:?WORD,
        ICCID:20/binary, Num:?BYTE, Length:?BYTE, Id/binary>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Seq">> => Seq,
        <<"ICCID">> => ICCID,
        <<"Num">> => Num,
        <<"Length">> => Length,
        <<"Id">> => Id
    };
parse_data(
    #frame{cmd = ?CMD_VIHECLE_LOGIN, proto_ver = ?PROTO_VER_2025},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Seq:?WORD,
        ICCID:20/binary, BmsNum:?BYTE, Rest/binary>>
) ->
    {BatteryPackPerBms, BatteryPackEncodings} = parse_bat_packs(BmsNum, Rest),
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Seq">> => Seq,
        <<"ICCID">> => ICCID,
        <<"BmsNum">> => BmsNum,
        <<"BatteryPackCounts">> => BatteryPackPerBms,
        <<"BatteryPackEncodings">> => BatteryPackEncodings
    };
parse_data(
    #frame{cmd = ?CMD_INFO_REPORT} = Frame,
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Infos/binary>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Infos">> => parse_info(Infos, [], Frame)
    };
parse_data(
    #frame{cmd = ?CMD_INFO_RE_REPORT} = Frame,
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Infos/binary>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Infos">> => parse_info(Infos, [], Frame)
    };
parse_data(
    #frame{cmd = ?CMD_VIHECLE_LOGOUT},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Seq:?WORD>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Seq">> => Seq
    };
parse_data(
    #frame{cmd = ?CMD_PLATFORM_LOGIN},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Seq:?WORD,
        Username:12/binary, Password:20/binary, Encrypt:?BYTE>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Seq">> => Seq,
        <<"Username">> => Username,
        <<"Password">> => Password,
        <<"Encrypt">> => Encrypt
    };
parse_data(
    #frame{cmd = ?CMD_PLATFORM_LOGOUT},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Seq:?WORD>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Seq">> => Seq
    };
parse_data(#frame{cmd = ?CMD_HEARTBEAT}, <<>>) ->
    #{};
parse_data(#frame{cmd = ?CMD_SCHOOL_TIME}, <<>>) ->
    #{};
parse_data(
    #frame{cmd = ?CMD_PARAM_QUERY, proto_ver = Version},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Total:?BYTE,
        Rest/binary>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Total">> => Total,
        <<"Params">> => parse_params(Rest, Version)
    };
parse_data(
    #frame{cmd = ?CMD_PARAM_SETTING, proto_ver = Version},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Total:?BYTE,
        Rest/binary>>
) ->
    ?SLOG(debug, #{msg => "rest", data => Rest}),
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Total">> => Total,
        <<"Params">> => parse_params(Rest, Version)
    };
parse_data(
    #frame{cmd = ?CMD_TERMINAL_CTRL},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Command:?BYTE,
        Rest/binary>>
) ->
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"Command">> => Command,
        <<"Param">> => parse_ctrl_param(Command, Rest)
    };
parse_data(
    #frame{cmd = ?CMD_ACTIVATION},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, ChipID:16/binary,
        PubKeyLen:?WORD, PubKey:PubKeyLen/binary, VIN:17/binary, Rest/binary>>
) ->
    {Signature, <<>>} = parse_signature(Rest),
    #{
        <<"Time">> => #{
            <<"Year">> => Year,
            <<"Month">> => Month,
            <<"Day">> => Day,
            <<"Hour">> => Hour,
            <<"Minute">> => Minute,
            <<"Second">> => Second
        },
        <<"ChipID">> => ChipID,
        <<"PubKeyLen">> => PubKeyLen,
        <<"PubKey">> => bin_to_hex(PubKey),
        <<"VIN">> => VIN,
        <<"Signature">> => Signature
    };
parse_data(
    #frame{cmd = ?CMD_KEY_EXCHANGE},
    <<KeyType:?BYTE, KeyLength:?WORD, Key:KeyLength/binary, ActiveYear:?BYTE, ActiveMonth:?BYTE,
        ActiveDay:?BYTE, ActiveHour:?BYTE, ActiveMinute:?BYTE, ActiveSecond:?BYTE, ExpireYear:?BYTE,
        ExpireMonth:?BYTE, ExpireDay:?BYTE, ExpireHour:?BYTE, ExpireMinute:?BYTE,
        ExpireSecond:?BYTE>>
) ->
    #{
        <<"KeyType">> => KeyType,
        <<"KeyLength">> => KeyLength,
        <<"Key">> => bin_to_hex(Key),
        <<"ActiveTime">> => #{
            <<"Year">> => ActiveYear,
            <<"Month">> => ActiveMonth,
            <<"Day">> => ActiveDay,
            <<"Hour">> => ActiveHour,
            <<"Minute">> => ActiveMinute,
            <<"Second">> => ActiveSecond
        },
        <<"ExpireTime">> => #{
            <<"Year">> => ExpireYear,
            <<"Month">> => ExpireMonth,
            <<"Day">> => ExpireDay,
            <<"Hour">> => ExpireHour,
            <<"Minute">> => ExpireMinute,
            <<"Second">> => ExpireSecond
        }
    };
parse_data(Frame, Data) ->
    ?SLOG(error, #{msg => "invalid_frame", frame => Frame, data => Data}),
    error(invalid_frame).

%%--------------------------------------------------------------------
%% Parse Report Data Info
%%--------------------------------------------------------------------

parse_info(<<>>, Acc, _Frame) ->
    lists:reverse(Acc);
%% ========================================
%% GBT32960-2016
parse_info(
    <<?INFO_TYPE_2016_VEHICLE, Body:20/binary, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    <<Status:?BYTE, Charging:?BYTE, Mode:?BYTE, Speed:?WORD, Mileage:?DWORD, Voltage:?WORD,
        Current:?WORD, SOC:?BYTE, DC:?BYTE, Gear:?BYTE, Resistance:?WORD, AcceleratorPedal:?BYTE,
        BrakePedal:?BYTE>> = Body,
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Vehicle">>,
                <<"Status">> => Status,
                <<"Charging">> => Charging,
                <<"Mode">> => Mode,
                <<"Speed">> => Speed,
                <<"Mileage">> => Mileage,
                <<"Voltage">> => Voltage,
                <<"Current">> => Current,
                <<"SOC">> => SOC,
                <<"DC">> => DC,
                <<"Gear">> => Gear,
                <<"Resistance">> => Resistance,
                <<"AcceleratorPedal">> => AcceleratorPedal,
                <<"BrakePedal">> => BrakePedal
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_DRIVE_MOTOR, Number, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    % 12 is packet len of per drive motor
    Len = Number * 12,
    <<Bodys:Len/binary, Rest1/binary>> = Rest,
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"DriveMotor">>,
                <<"Number">> => Number,
                <<"Motors">> => parse_drive_motor(Bodys, [], ?PROTO_VER_2016)
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_FUEL_CELL, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    <<CellVoltage:?WORD, CellCurrent:?WORD, FuelConsumption:?WORD, ProbeNum:?WORD, Rest1/binary>> =
        Rest,

    <<ProbeTemps:ProbeNum/binary, Rest2/binary>> = Rest1,

    <<HMaxTemp:?WORD, HTempProbeCode:?BYTE, HMaxConc:?WORD, HConcSensorCode:?BYTE, HMaxPress:?WORD,
        HPressSensorCode:?BYTE, DCStatus:?BYTE, Rest3/binary>> = Rest2,
    parse_info(
        Rest3,
        [
            #{
                <<"Type">> => <<"FuelCell">>,
                <<"CellVoltage">> => CellVoltage,
                <<"CellCurrent">> => CellCurrent,
                <<"FuelConsumption">> => FuelConsumption,
                <<"ProbeNum">> => ProbeNum,
                <<"ProbeTemps">> => binary_to_list(ProbeTemps),
                <<"H_MaxTemp">> => HMaxTemp,
                <<"H_TempProbeCode">> => HTempProbeCode,
                <<"H_MaxConc">> => HMaxConc,
                <<"H_ConcSensorCode">> => HConcSensorCode,
                <<"H_MaxPress">> => HMaxPress,
                <<"H_PressSensorCode">> => HPressSensorCode,
                <<"DCStatus">> => DCStatus
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_ENGINE, Status:?BYTE, CrankshaftSpeed:?WORD, FuelConsumption:?WORD,
        Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Engine">>,
                <<"Status">> => Status,
                <<"CrankshaftSpeed">> => CrankshaftSpeed,
                <<"FuelConsumption">> => FuelConsumption
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_LOCATION, Status:?BYTE, Longitude:?DWORD, Latitude:?DWORD, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Location">>,
                <<"Status">> => Status,
                <<"Longitude">> => Longitude,
                <<"Latitude">> => Latitude
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_EXTREME, Body:14/binary, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    <<MaxVoltageBatterySubsysNo:?BYTE, MaxVoltageBatteryCode:?BYTE, MaxBatteryVoltage:?WORD,
        MinVoltageBatterySubsysNo:?BYTE, MinVoltageBatteryCode:?BYTE, MinBatteryVoltage:?WORD,
        MaxTempSubsysNo:?BYTE, MaxTempProbeNo:?BYTE, MaxTemp:?BYTE, MinTempSubsysNo:?BYTE,
        MinTempProbeNo:?BYTE, MinTemp:?BYTE>> = Body,

    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Extreme">>,
                <<"MaxVoltageBatterySubsysNo">> => MaxVoltageBatterySubsysNo,
                <<"MaxVoltageBatteryCode">> => MaxVoltageBatteryCode,
                <<"MaxBatteryVoltage">> => MaxBatteryVoltage,
                <<"MinVoltageBatterySubsysNo">> => MinVoltageBatterySubsysNo,
                <<"MinVoltageBatteryCode">> => MinVoltageBatteryCode,
                <<"MinBatteryVoltage">> => MinBatteryVoltage,
                <<"MaxTempSubsysNo">> => MaxTempSubsysNo,
                <<"MaxTempProbeNo">> => MaxTempProbeNo,
                <<"MaxTemp">> => MaxTemp,
                <<"MinTempSubsysNo">> => MinTempSubsysNo,
                <<"MinTempProbeNo">> => MinTempProbeNo,
                <<"MinTemp">> => MinTemp
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_ALARM, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    <<MaxAlarmLevel:?BYTE, GeneralAlarmFlag:?DWORD, FaultChargeableDeviceNum:?BYTE, Rest1/binary>> =
        Rest,
    N1 = FaultChargeableDeviceNum * 4,
    <<FaultChargeableDeviceList:N1/binary, FaultDriveMotorNum:?BYTE, Rest2/binary>> = Rest1,
    N2 = FaultDriveMotorNum * 4,
    <<FaultDriveMotorList:N2/binary, FaultEngineNum:?BYTE, Rest3/binary>> = Rest2,
    N3 = FaultEngineNum * 4,
    <<FaultEngineList:N3/binary, FaultOthersNum:?BYTE, Rest4/binary>> = Rest3,
    N4 = FaultOthersNum * 4,
    <<FaultOthersList:N4/binary, Rest5/binary>> = Rest4,
    parse_info(
        Rest5,
        [
            #{
                <<"Type">> => <<"Alarm">>,
                <<"MaxAlarmLevel">> => MaxAlarmLevel,
                <<"GeneralAlarmFlag">> => GeneralAlarmFlag,
                <<"FaultChargeableDeviceNum">> => FaultChargeableDeviceNum,
                <<"FaultChargeableDeviceList">> => tune_fault_codelist(
                    FaultChargeableDeviceList, ?PROTO_VER_2016
                ),
                <<"FaultDriveMotorNum">> => FaultDriveMotorNum,
                <<"FaultDriveMotorList">> => tune_fault_codelist(
                    FaultDriveMotorList, ?PROTO_VER_2016
                ),
                <<"FaultEngineNum">> => FaultEngineNum,
                <<"FaultEngineList">> => tune_fault_codelist(FaultEngineList, ?PROTO_VER_2016),
                <<"FaultOthersNum">> => FaultOthersNum,
                <<"FaultOthersList">> => tune_fault_codelist(FaultOthersList, ?PROTO_VER_2016)
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_CHARGEABLE_BAT_VOLTAGE, Number:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    {Rest1, SubSystems} = parse_chargeable_voltage(Rest, Number, []),
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"ChargeableVoltage">>,
                <<"Number">> => Number,
                <<"SubSystems">> => SubSystems
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2016_CHARGEABLE_BAT_TEMP, Number:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2016
) ->
    {Rest1, SubSystems} = parse_chargeable_temp(Rest, Number, []),
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"ChargeableTemp">>,
                <<"Number">> => Number,
                <<"SubSystems">> => SubSystems
            }
            | Acc
        ],
        Frame
    );
%% END GBT32960-2016
%% ========================================

%% ========================================
%% GBT32960-2025

parse_info(
    <<?INFO_TYPE_2025_VEHICLE, Body:18/binary, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    <<Status:?BYTE, Charging:?BYTE, Mode:?BYTE, Speed:?WORD, Mileage:?DWORD, Voltage:?WORD,
        Current:?WORD, SOC:?BYTE, DC:?BYTE, Gear:?BYTE, Resistance:?WORD>> = Body,
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Vehicle">>,
                <<"Status">> => Status,
                <<"Charging">> => Charging,
                <<"Mode">> => Mode,
                <<"Speed">> => Speed,
                <<"Mileage">> => Mileage,
                <<"Voltage">> => Voltage,
                <<"Current">> => Current,
                <<"SOC">> => SOC,
                <<"DC">> => DC,
                <<"Gear">> => Gear,
                <<"Resistance">> => Resistance
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_DRIVE_MOTOR, Number, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    % 10 is packet len of per drive motor in proto 2025
    Len = Number * 10,
    <<Bodys:Len/binary, Rest1/binary>> = Rest,
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"DriveMotor">>,
                <<"Number">> => Number,
                <<"Motors">> => parse_drive_motor(Bodys, [], ?PROTO_VER_2025)
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_FUEL_CELL, Body:12/binary, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    <<HMaxTemp:?WORD, HTempProbeCode:?BYTE, HMaxConc:?WORD, HConcSensorCode:?BYTE, HMaxPress:?WORD,
        HPressSensorCode:?BYTE, DCStatus:?BYTE, RemainingH2:?BYTE, DCDCTemp:?BYTE>> = Body,
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"FuelCell">>,
                <<"H_MaxTemp">> => HMaxTemp,
                <<"H_TempProbeCode">> => HTempProbeCode,
                <<"H_MaxConc">> => HMaxConc,
                <<"H_ConcSensorCode">> => HConcSensorCode,
                <<"H_MaxPress">> => HMaxPress,
                <<"H_PressSensorCode">> => HPressSensorCode,
                <<"DCStatus">> => DCStatus,
                <<"RemainingH2">> => RemainingH2,
                <<"DCDCTemp">> => DCDCTemp
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_ENGINE, CrankshaftSpeed:?WORD, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Engine">>,
                <<"CrankshaftSpeed">> => CrankshaftSpeed
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_LOCATION, Status:?BYTE, CoordinateSystem:?BYTE, Longitude:?DWORD,
        Latitude:?DWORD, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"Location">>,
                <<"Status">> => Status,
                <<"CoordinateSystem">> => CoordinateSystem,
                <<"Longitude">> => Longitude,
                <<"Latitude">> => Latitude
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_ALARM, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    <<MaxAlarmLevel:?BYTE, GeneralAlarmFlag:?DWORD, FaultChargeableDeviceNum:?BYTE, Rest1/binary>> =
        Rest,
    N1 = FaultChargeableDeviceNum * 4,
    <<FaultChargeableDeviceList:N1/binary, FaultDriveMotorNum:?BYTE, Rest2/binary>> = Rest1,
    N2 = FaultDriveMotorNum * 4,
    <<FaultDriveMotorList:N2/binary, FaultEngineNum:?BYTE, Rest3/binary>> = Rest2,
    N3 = FaultEngineNum * 4,
    <<FaultEngineList:N3/binary, FaultOthersNum:?BYTE, Rest4/binary>> = Rest3,
    N4 = FaultOthersNum * 4,
    <<FaultOthersList:N4/binary, FaultGeneralNum:?BYTE, Rest5/binary>> = Rest4,
    N5 = FaultGeneralNum * 2,
    <<FaultGeneralLevelList:N5/binary, Rest6/binary>> = Rest5,
    parse_info(
        Rest6,
        [
            #{
                <<"Type">> => <<"Alarm">>,
                <<"MaxAlarmLevel">> => MaxAlarmLevel,
                <<"GeneralAlarmFlag">> => GeneralAlarmFlag,
                <<"FaultChargeableDeviceNum">> => FaultChargeableDeviceNum,
                <<"FaultChargeableDeviceList">> => tune_fault_codelist(
                    FaultChargeableDeviceList, ?PROTO_VER_2025
                ),
                <<"FaultDriveMotorNum">> => FaultDriveMotorNum,
                <<"FaultDriveMotorList">> => tune_fault_codelist(
                    FaultDriveMotorList, ?PROTO_VER_2025
                ),
                <<"FaultEngineNum">> => FaultEngineNum,
                <<"FaultEngineList">> => tune_fault_codelist(FaultEngineList, ?PROTO_VER_2025),
                <<"FaultOthersNum">> => FaultOthersNum,
                <<"FaultOthersList">> => tune_fault_codelist(FaultOthersList, ?PROTO_VER_2025),
                <<"FaultGeneralNum">> => FaultGeneralNum,
                <<"FaultGeneralList">> => tune_fault_level(FaultGeneralLevelList)
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_POWER_BATTERY_VOLTAGE, Number:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    {Rest1, SubSystems} = parse_power_battery_voltage(Rest, Number, []),
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"MinVoltageOfPowerBattery">>,
                <<"Number">> => Number,
                <<"SubSystems">> => SubSystems
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_POWER_BATTERY_TEMP, Number:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    {Rest1, SubSystems} = parse_power_battery_temp(Rest, Number, []),
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"TempOfPowerBattery">>,
                <<"Number">> => Number,
                <<"SubSystems">> => SubSystems
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_FUEL_CELL_STACK, Number:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    {Rest1, Stacks} = parse_fuel_cell_stack(Rest, Number, []),
    parse_info(
        Rest1,
        [
            #{
                <<"Type">> => <<"FuelCellStack">>,
                <<"Number">> => Number,
                <<"Stacks">> => Stacks
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_SUPER_CAPACITOR, ManagerSysNo:?BYTE, TotalVoltage:?WORD, TotalCurrent:?WORD,
        CellsTotal:?WORD, Rest1/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    LenM = CellsTotal * 2,
    <<CellsVoltage:LenM/binary, ProbeNum:?WORD, Rest2/binary>> = Rest1,
    LenN = ProbeNum,
    <<ProbeTemp:LenN/binary, Rest3/binary>> = Rest2,
    parse_info(
        Rest3,
        [
            #{
                <<"Type">> => <<"SuperCapacitor">>,
                <<"ManagerSysNo">> => ManagerSysNo,
                <<"TotalVoltage">> => TotalVoltage,
                <<"TotalCurrent">> => TotalCurrent,
                <<"CellsTotal">> => CellsTotal,
                <<"CellsVoltage">> => tune_voltage(CellsVoltage),
                <<"ProbeNum">> => ProbeNum,
                <<"ProbeTemp">> => binary_to_list(ProbeTemp)
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_SUPER_CAPACITOR_EXTREME, MaxVoltageManagerSysNo:?BYTE,
        MaxVoltageCellCode:?WORD, MaxVoltageCellValue:?WORD, MinVoltageManagerSysNo:?BYTE,
        MinVoltageCellCode:?WORD, MinVoltageCellValue:?WORD, MaxTempManagerSysNo:?BYTE,
        MaxTempProbeCode:?WORD, MaxTempValue:?BYTE, MinTempManagerSysNo:?BYTE,
        MinTempProbeCode:?WORD, MinTempValue:?BYTE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    parse_info(
        Rest,
        [
            #{
                <<"Type">> => <<"SuperCapacitorExtreme">>,
                <<"MaxVoltageManagerSysNo">> => MaxVoltageManagerSysNo,
                <<"MaxVoltageCellCode">> => MaxVoltageCellCode,
                <<"MaxVoltageCellValue">> => MaxVoltageCellValue,
                <<"MinVoltageManagerSysNo">> => MinVoltageManagerSysNo,
                <<"MinVoltageCellCode">> => MinVoltageCellCode,
                <<"MinVoltageCellValue">> => MinVoltageCellValue,
                <<"MaxTempManagerSysNo">> => MaxTempManagerSysNo,
                <<"MaxTempProbeCode">> => MaxTempProbeCode,
                <<"MaxTempValue">> => MaxTempValue,
                <<"MinTempManagerSysNo">> => MinTempManagerSysNo,
                <<"MinTempProbeCode">> => MinTempProbeCode,
                <<"MinTempValue">> => MinTempValue
            }
            | Acc
        ],
        Frame
    );
parse_info(
    <<?INFO_TYPE_2025_SIGNATURE, Rest/binary>>,
    Acc,
    Frame = ?IS_PROTO_VER_2025
) ->
    {Signature, Rest1} = parse_signature(Rest),
    parse_info(Rest1, [Signature | Acc], Frame);
%% END GBT32960-2025
%% ========================================

parse_info(Rest, Acc, _Frame) ->
    ?SLOG(warning, #{msg => "customized_info_type", rest => Rest}),
    lists:reverse(
        [
            #{
                <<"Type">> => <<"CustomData">>,
                <<"Data">> => base64:encode(Rest)
            }
            | Acc
        ]
    ).

parse_drive_motor(<<>>, Acc, _Ver) ->
    lists:reverse(Acc);
parse_drive_motor(
    <<No:?BYTE, Status:?BYTE, CtrlTemp:?BYTE, Rotating:?WORD, Torque:?WORD, MotorTemp:?BYTE,
        InputVoltage:?WORD, DCBusCurrent:?WORD, Rest/binary>>,
    Acc,
    ?PROTO_VER_2016
) ->
    parse_drive_motor(
        Rest,
        [
            #{
                <<"No">> => No,
                <<"Status">> => Status,
                <<"CtrlTemp">> => CtrlTemp,
                <<"Rotating">> => Rotating,
                <<"Torque">> => Torque,
                <<"MotorTemp">> => MotorTemp,
                <<"InputVoltage">> => InputVoltage,
                <<"DCBusCurrent">> => DCBusCurrent
            }
            | Acc
        ],
        ?PROTO_VER_2016
    );
parse_drive_motor(
    <<No:?BYTE, Status:?BYTE, CtrlTemp:?BYTE, Rotating:?WORD, Torque:?DWORD, MotorTemp:?BYTE,
        Rest/binary>>,
    Acc,
    ?PROTO_VER_2025
) ->
    parse_drive_motor(
        Rest,
        [
            #{
                <<"No">> => No,
                <<"Status">> => Status,
                <<"CtrlTemp">> => CtrlTemp,
                <<"Rotating">> => Rotating,
                <<"Torque">> => Torque,
                <<"MotorTemp">> => MotorTemp
            }
            | Acc
        ],
        ?PROTO_VER_2025
    ).

parse_chargeable_voltage(Rest, 0, Acc) ->
    {Rest, lists:reverse(Acc)};
parse_chargeable_voltage(
    <<ChargeableSubsysNo:?BYTE, ChargeableVoltage:?WORD, ChargeableCurrent:?WORD, CellsTotal:?WORD,
        FrameCellsIndex:?WORD, FrameCellsCount:?BYTE, Rest/binary>>,
    Num,
    Acc
) ->
    Len = FrameCellsCount * 2,
    <<CellsVoltage:Len/binary, Rest1/binary>> = Rest,
    parse_chargeable_voltage(Rest1, Num - 1, [
        #{
            <<"ChargeableSubsysNo">> => ChargeableSubsysNo,
            <<"ChargeableVoltage">> => ChargeableVoltage,
            <<"ChargeableCurrent">> => ChargeableCurrent,
            <<"CellsTotal">> => CellsTotal,
            <<"FrameCellsIndex">> => FrameCellsIndex,
            <<"FrameCellsCount">> => FrameCellsCount,
            <<"CellsVoltage">> => tune_voltage(CellsVoltage)
        }
        | Acc
    ]).

parse_chargeable_temp(Rest, 0, Acc) ->
    {Rest, lists:reverse(Acc)};
parse_chargeable_temp(<<ChargeableSubsysNo:?BYTE, ProbeNum:?WORD, Rest/binary>>, Num, Acc) ->
    <<ProbesTemp:ProbeNum/binary, Rest1/binary>> = Rest,
    parse_chargeable_temp(Rest1, Num - 1, [
        #{
            <<"ChargeableSubsysNo">> => ChargeableSubsysNo,
            <<"ProbeNum">> => ProbeNum,
            <<"ProbesTemp">> => binary_to_list(ProbesTemp)
        }
        | Acc
    ]).

%% Only for PROTO_VER_2025 VLOGIN frames
parse_bat_packs(BatMgmtSysNum, Bin) ->
    parse_bat_packs(BatMgmtSysNum, Bin, []).

parse_bat_packs(0, Rest, MAcc) ->
    MList = lists:reverse(MAcc),
    {MList, parse_bat_encodings(MList, Rest, [])};
parse_bat_packs(BasNum, <<M:?BYTE, Rest/binary>>, MAcc) ->
    parse_bat_packs(BasNum - 1, Rest, [M | MAcc]).

parse_bat_encodings([], RestBin, _) when RestBin =/= <<>> ->
    ?SLOG(error, #{msg => "invalid_bat_pack_encoding", rest => RestBin}),
    error(invalid_bat_pack_encoding);
parse_bat_encodings([], <<>>, Acc) ->
    lists:reverse(Acc);
parse_bat_encodings([M | RestM], Bin, Acc) ->
    {BmsEncodings, RestBin} = parse_bms_encodings(M, Bin, []),
    parse_bat_encodings(RestM, RestBin, [BmsEncodings | Acc]).

-define(BAT_PACK_CODE_LEN, 24).

parse_bms_encodings(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
parse_bms_encodings(M, <<Encoding:?BAT_PACK_CODE_LEN/binary, RestBin/binary>>, Acc) ->
    parse_bms_encodings(M - 1, RestBin, [Encoding | Acc]).

parse_power_battery_voltage(Rest, 0, Acc) ->
    {Rest, lists:reverse(Acc)};
parse_power_battery_voltage(
    <<BatteryPackNo:?BYTE, BatteryPackVoltage:?WORD, BatteryPackCurrent:?WORD,
        MinParallelUnitTotal:?WORD, Rest/binary>>,
    Num,
    Acc
) ->
    Len = MinParallelUnitTotal * 2,
    <<MinParallelUnitVoltage:Len/binary, Rest1/binary>> = Rest,
    parse_power_battery_voltage(Rest1, Num - 1, [
        #{
            <<"BatteryPackNo">> => BatteryPackNo,
            <<"BatteryPackVoltage">> => BatteryPackVoltage,
            <<"BatteryPackCurrent">> => BatteryPackCurrent,
            <<"MinParallelUnitTotal">> => MinParallelUnitTotal,
            <<"MinParallelUnitVoltage">> => tune_voltage(MinParallelUnitVoltage)
        }
        | Acc
    ]).

parse_power_battery_temp(Rest, 0, Acc) ->
    {Rest, lists:reverse(Acc)};
parse_power_battery_temp(<<BatteryPackNo:?BYTE, ProbeNum:?WORD, Rest/binary>>, Num, Acc) ->
    <<ProbesTemp:ProbeNum/binary, Rest1/binary>> = Rest,
    parse_power_battery_temp(Rest1, Num - 1, [
        #{
            <<"BatteryPackNo">> => BatteryPackNo,
            <<"ProbeNum">> => ProbeNum,
            <<"ProbesTemp">> => binary_to_list(ProbesTemp)
        }
        | Acc
    ]).

parse_fuel_cell_stack(Rest, 0, Acc) ->
    {Rest, lists:reverse(Acc)};
parse_fuel_cell_stack(
    <<FuelCellStackNo:?BYTE, Voltage:?WORD, Current:?WORD, H2InletPressure:?WORD,
        AirInletPressure:?WORD, AirInletTemp:?BYTE, StackProbeNum:?WORD, Rest1/binary>>,
    Num,
    Acc
) ->
    Len = StackProbeNum,
    <<StackProbeTemp:Len/binary, Rest2/binary>> = Rest1,
    parse_fuel_cell_stack(Rest2, Num - 1, [
        #{
            <<"FuelCellStackNo">> => FuelCellStackNo,
            <<"Voltage">> => Voltage,
            <<"Current">> => Current,
            <<"H2InletPressure">> => H2InletPressure,
            <<"AirInletPressure">> => AirInletPressure,
            <<"AirInletTemp">> => AirInletTemp,
            <<"StackProbeNum">> => StackProbeNum,
            <<"StackProbeTemp">> => binary_to_list(StackProbeTemp)
        }
        | Acc
    ]).

parse_signature(
    <<SigType:?BYTE, RLen:?WORD, RVal:RLen/binary, SLen:?WORD, SVal:SLen/binary, Rest/binary>>
) ->
    {
        #{
            <<"Type">> => <<"Signature">>,
            <<"SignatureType">> => SigType,
            <<"RLength">> => RLen,
            <<"RValue">> => bin_to_hex(RVal),
            <<"SLength">> => SLen,
            <<"SValue">> => bin_to_hex(SVal)
        },
        Rest
    }.

tune_fault_codelist(<<>>, _Ver) ->
    [];
tune_fault_codelist(Data, ?PROTO_VER_2025) ->
    lists:flatten([list_to_binary(io_lib:format("~8.16.0B", [X])) || <<X:?DWORD>> <= Data]);
tune_fault_codelist(Data, _Ver) ->
    lists:flatten([list_to_binary(io_lib:format("~4.16.0B", [X])) || <<X:?DWORD>> <= Data]).

tune_fault_level(<<>>) ->
    [];
tune_fault_level(Data) ->
    [#{<<"No">> => No, <<"Level">> => Level} || <<No:?BYTE, Level:?BYTE>> <= Data].

bin_to_hex(Bin) ->
    list_to_binary([io_lib:format("~2.16.0B", [B]) || <<B>> <= Bin]).

hex_to_bin(Hex) ->
    <<<<(binary_to_integer(<<H1, H2>>, 16))>> || <<H1, H2>> <= Hex>>.

tune_voltage(Bin) -> tune_voltage_(Bin, []).
tune_voltage_(<<>>, Acc) -> lists:reverse(Acc);
tune_voltage_(<<V:?WORD, Rest/binary>>, Acc) -> tune_voltage_(Rest, [V | Acc]).

parse_params(Bin, Version) -> parse_params_(Bin, [], Version).
parse_params_(<<>>, Acc, _Version) ->
    lists:reverse(Acc);
parse_params_(<<16#01, Val:?WORD, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x01">> => Val} | Acc], Ver);
parse_params_(<<16#02, Val:?WORD, Rest/binary>>, Acc, ?PROTO_VER_2016) ->
    parse_params_(Rest, [#{<<"0x02">> => Val} | Acc], ?PROTO_VER_2016);
parse_params_(<<16#02, Val:?BYTE, Rest/binary>>, Acc, ?PROTO_VER_2025) ->
    parse_params_(Rest, [#{<<"0x02">> => Val} | Acc], ?PROTO_VER_2025);
parse_params_(<<16#03, Val:?WORD, Rest/binary>>, Acc, ?PROTO_VER_2016) ->
    parse_params_(Rest, [#{<<"0x03">> => Val} | Acc], ?PROTO_VER_2016);
parse_params_(<<16#03, Val:?BYTE, Rest/binary>>, Acc, ?PROTO_VER_2025) ->
    parse_params_(Rest, [#{<<"0x03">> => Val} | Acc], ?PROTO_VER_2025);
parse_params_(<<16#04, Val:?BYTE, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x04">> => Val} | Acc], Ver);
parse_params_(<<16#05, Rest/binary>>, Acc, Ver) ->
    case [V || #{<<"0x04">> := V} <- Acc] of
        [Len] ->
            <<Val:Len/binary, Rest1/binary>> = Rest,
            parse_params_(Rest1, [#{<<"0x05">> => Val} | Acc], Ver);
        _ ->
            ?SLOG(error, #{
                msg => "invalid_data", reason => "cmd_0x04 must appear ahead of cmd_0x05"
            }),
            lists:reverse(Acc)
    end;
parse_params_(<<16#06, Val:?WORD, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x06">> => Val} | Acc], Ver);
parse_params_(<<16#07, Val:5/binary, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x07">> => Val} | Acc], Ver);
parse_params_(<<16#08, Val:5/binary, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x08">> => Val} | Acc], Ver);
parse_params_(<<16#09, Val:?BYTE, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x09">> => Val} | Acc], Ver);
parse_params_(<<16#0A, Val:?WORD, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x0A">> => Val} | Acc], Ver);
parse_params_(<<16#0B, Val:?WORD, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x0B">> => Val} | Acc], Ver);
parse_params_(<<16#0C, Val:?BYTE, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x0C">> => Val} | Acc], Ver);
parse_params_(<<16#0D, Val:?BYTE, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x0D">> => Val} | Acc], Ver);
parse_params_(<<16#0E, Rest/binary>>, Acc, Ver) ->
    case [V || #{<<"0x0D">> := V} <- Acc] of
        [Len] ->
            <<Val:Len/binary, Rest1/binary>> = Rest,
            parse_params_(Rest1, [#{<<"0x0E">> => Val} | Acc], Ver);
        _ ->
            ?SLOG(error, #{
                msg => "invalid_data", reason => "cmd_0x0D must appear ahead of cmd_0x0E"
            }),
            lists:reverse(Acc)
    end;
parse_params_(<<16#0F, Val:?WORD, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x0F">> => Val} | Acc], Ver);
parse_params_(<<16#10, Val:?BYTE, Rest/binary>>, Acc, Ver) ->
    parse_params_(Rest, [#{<<"0x10">> => Val} | Acc], Ver);
parse_params_(Cmd, Acc, _Ver) ->
    ?SLOG(error, #{msg => "unexpected_param_identifier", cmd => Cmd}),
    lists:reverse(Acc).

parse_ctrl_param(16#01, Param) ->
    parse_upgrade_feild(Param);
parse_ctrl_param(16#02, _) ->
    <<>>;
parse_ctrl_param(16#03, _) ->
    <<>>;
parse_ctrl_param(16#04, _) ->
    <<>>;
parse_ctrl_param(16#05, _) ->
    <<>>;
parse_ctrl_param(16#06, <<Level:?BYTE, Msg/binary>>) ->
    #{<<"Level">> => Level, <<"Message">> => Msg};
parse_ctrl_param(16#07, _) ->
    <<>>;
parse_ctrl_param(Cmd, Param) ->
    ?SLOG(error, #{msg => "unexpected_param", param => Param, cmd => Cmd}),
    <<>>.

parse_upgrade_feild(Param) ->
    [
        DialingName,
        Username,
        Password,
        <<0, 0, I1, I2, I3, I4>>,
        <<Port:?WORD>>,
        ManufacturerId,
        HardwareVer,
        SoftwareVer,
        UpgradeUrl,
        <<Timeout:?WORD>>
    ] = re:split(Param, ";", [{return, binary}]),

    #{
        <<"DialingName">> => DialingName,
        <<"Username">> => Username,
        <<"Password">> => Password,
        <<"Ip">> => list_to_binary(inet:ntoa({I1, I2, I3, I4})),
        <<"Port">> => Port,
        <<"ManufacturerId">> => ManufacturerId,
        <<"HardwareVer">> => HardwareVer,
        <<"SoftwareVer">> => SoftwareVer,
        <<"UpgradeUrl">> => UpgradeUrl,
        <<"Timeout">> => Timeout
    }.

%%--------------------------------------------------------------------
%% serialize_pkt
%%--------------------------------------------------------------------
serialize_pkt(Frame, _Opts) ->
    serialize(Frame).

serialize(#frame{
    cmd = Cmd,
    ack = Ack,
    vin = VIN,
    encrypt = Encrypt,
    data = Data,
    rawdata = RawData,
    proto_ver = Version
}) ->
    Encrypted = encipher(serialize_data(Cmd, Ack, RawData, Data, Version), Encrypt),
    Len = byte_size(Encrypted),
    Stream = <<Cmd:?BYTE, Ack:?BYTE, VIN:17/binary, Encrypt:?BYTE, Len:?WORD, Encrypted/binary>>,
    Crc = cal_check(Stream, byte_size(Stream), undefined),
    Start =
        case Version of
            ?PROTO_VER_2016 -> <<"##">>;
            ?PROTO_VER_2025 -> <<"$$">>
        end,
    <<Start/binary, Stream/binary, Crc:?BYTE>>.

serialize_data(
    ?CMD_PARAM_QUERY,
    ?ACK_IS_CMD,
    _,
    #{
        <<"Time">> := Time,
        <<"Total">> := Total,
        <<"Ids">> := Ids
    },
    _Version
) when length(Ids) == Total ->
    T = tune_time(Time),
    Ids1 = tune_ids(Ids),
    <<T/binary, Total:?BYTE, Ids1/binary>>;
serialize_data(
    ?CMD_PARAM_QUERY,
    Ack,
    _,
    #{
        <<"Time">> := Time,
        <<"Total">> := Total,
        <<"Params">> := Params
    },
    Version
) when ?IS_RESPONSE(Ack) ->
    T = tune_time(Time),
    Params1 = tune_params(Params, Version),
    <<T/binary, Total:?BYTE, Params1/binary>>;
serialize_data(
    ?CMD_PARAM_SETTING,
    _Ack,
    _,
    #{
        <<"Time">> := Time,
        <<"Total">> := Total,
        <<"Params">> := Params
    },
    Version
) when length(Params) == Total ->
    T = tune_time(Time),
    Params1 = tune_params(Params, Version),
    <<T/binary, Total:?BYTE, Params1/binary>>;
serialize_data(
    ?CMD_TERMINAL_CTRL,
    ?ACK_IS_CMD,
    _,
    #{
        <<"Time">> := Time,
        <<"Command">> := Cmd,
        <<"Param">> := Param
    },
    _Version
) ->
    T = tune_time(Time),
    Param1 = tune_ctrl_param(Cmd, Param),
    <<T/binary, Cmd:?BYTE, Param1/binary>>;
serialize_data(
    ?CMD_ACTIVATION_RES,
    ?ACK_IS_CMD,
    _,
    #{
        <<"ActivationStatus">> := Status,
        <<"Info">> := Info
    },
    _Version
) ->
    <<Status:?BYTE, Info:?BYTE>>;
serialize_data(
    ?CMD_KEY_EXCHANGE,
    _Ack,
    _RawData,
    #{
        <<"KeyType">> := KeyType,
        <<"KeyLength">> := KeyLength,
        <<"Key">> := Key,
        <<"ActiveTime">> := ActiveTime,
        <<"ExpireTime">> := ExpireTime
    },
    _Version
) ->
    ActiveTimeBin = tune_time(ActiveTime),
    ExpireTimeBin = tune_time(ExpireTime),
    KeyBin = hex_to_bin(Key),
    <<KeyType:?BYTE, KeyLength:?WORD, KeyBin/binary, ActiveTimeBin/binary, ExpireTimeBin/binary>>;
serialize_data(_Cmd, Ack, RawData, #{<<"Time">> := Time}, _Version) when ?IS_RESPONSE(Ack) ->
    Rest =
        case is_binary(RawData) andalso byte_size(RawData) > 6 of
            false -> <<>>;
            true -> binary:part(RawData, 6, byte_size(RawData) - 6)
        end,
    T = tune_time(Time),
    <<T/binary, Rest/binary>>.

tune_time(#{
    <<"Year">> := Year,
    <<"Month">> := Month,
    <<"Day">> := Day,
    <<"Hour">> := Hour,
    <<"Minute">> := Min,
    <<"Second">> := Sec
}) ->
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Min:?BYTE, Sec:?BYTE>>.

tune_ids(Ids) ->
    lists:foldr(
        fun
            (Id, Acc) when is_integer(Id) ->
                <<Id:8, Acc/binary>>;
            (Id, Acc) when is_binary(Id) ->
                <<Id/binary, Acc/binary>>
        end,
        <<>>,
        Ids
    ).

tune_params(Params, Version) ->
    tune_params_(lists:reverse(Params), <<>>, Version).

tune_params_([], Bin, _Version) ->
    Bin;
tune_params_([#{16#01 := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#01:?BYTE, Val:?WORD, Bin/binary>>, Ver);
tune_params_([#{16#02 := Val} | Rest], Bin, ?PROTO_VER_2016) ->
    tune_params_(Rest, <<16#02:?BYTE, Val:?WORD, Bin/binary>>, ?PROTO_VER_2016);
tune_params_([#{16#02 := Val} | Rest], Bin, ?PROTO_VER_2025) ->
    tune_params_(Rest, <<16#02:?BYTE, Val:?BYTE, Bin/binary>>, ?PROTO_VER_2025);
tune_params_([#{16#03 := Val} | Rest], Bin, ?PROTO_VER_2016) ->
    tune_params_(Rest, <<16#03:?BYTE, Val:?WORD, Bin/binary>>, ?PROTO_VER_2016);
tune_params_([#{16#03 := Val} | Rest], Bin, ?PROTO_VER_2025) ->
    tune_params_(Rest, <<16#03:?BYTE, Val:?BYTE, Bin/binary>>, ?PROTO_VER_2025);
tune_params_([#{16#04 := Val} | Rest], Bin, Ver) ->
    {Val05, Rest1} = take_param(16#05, Rest),
    tune_params_(Rest1, <<16#04:?BYTE, Val:?BYTE, 16#05, Val05:Val/binary, Bin/binary>>, Ver);
tune_params_([#{16#05 := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest ++ [#{16#05 => Val}], Bin, Ver);
tune_params_([#{16#06 := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#06:?BYTE, Val:?WORD, Bin/binary>>, Ver);
tune_params_([#{16#07 := Val} | Rest], Bin, Ver) when byte_size(Val) == 5 ->
    tune_params_(Rest, <<16#07:?BYTE, Val/binary, Bin/binary>>, Ver);
tune_params_([#{16#08 := Val} | Rest], Bin, Ver) when byte_size(Val) == 5 ->
    tune_params_(Rest, <<16#08:?BYTE, Val/binary, Bin/binary>>, Ver);
tune_params_([#{16#09 := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#09:?BYTE, Val:?BYTE, Bin/binary>>, Ver);
tune_params_([#{16#0A := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#0A:?BYTE, Val:?WORD, Bin/binary>>, Ver);
tune_params_([#{16#0B := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#0B:?BYTE, Val:?WORD, Bin/binary>>, Ver);
tune_params_([#{16#0C := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#0C:?BYTE, Val:?BYTE, Bin/binary>>, Ver);
tune_params_([#{16#0D := Val} | Rest], Bin, Ver) ->
    {Val0E, Rest1} = take_param(16#0E, Rest),
    tune_params_(Rest1, <<16#0D:?BYTE, Val:?BYTE, 16#0E, Val0E:Val/binary, Bin/binary>>, Ver);
tune_params_([#{16#0E := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest ++ [#{16#0E => Val}], Bin, Ver);
tune_params_([#{16#0F := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#0F:?BYTE, Val:?WORD, Bin/binary>>, Ver);
tune_params_([#{16#10 := Val} | Rest], Bin, Ver) ->
    tune_params_(Rest, <<16#10:?BYTE, Val:?BYTE, Bin/binary>>, Ver).

tune_ctrl_param(16#00, _) ->
    <<>>;
tune_ctrl_param(16#01, Param) ->
    tune_upgrade_feild(Param);
tune_ctrl_param(16#02, _) ->
    <<>>;
tune_ctrl_param(16#03, _) ->
    <<>>;
tune_ctrl_param(16#04, _) ->
    <<>>;
tune_ctrl_param(16#05, _) ->
    <<>>;
tune_ctrl_param(16#06, #{<<"Level">> := Level, <<"Message">> := Msg}) ->
    <<Level:?BYTE, Msg/binary>>;
tune_ctrl_param(16#07, _) ->
    <<>>;
tune_ctrl_param(Cmd, Param) ->
    ?SLOG(error, #{msg => "unexpected_cmd", cmd => Cmd, param => Param}),
    <<>>.

tune_upgrade_feild(Param) ->
    TuneBin = fun
        (Bin, Len) when is_binary(Bin), byte_size(Bin) =:= Len -> Bin;
        (undefined, _) -> undefined;
        (Bin, _) -> error({invalid_param_length, Bin})
    end,
    TuneWrd = fun
        (Val) when is_integer(Val), Val < 65535 -> <<Val:?WORD>>;
        (undefined) -> undefined;
        (_) -> error(invalid_param_word_value)
    end,
    TuneAdr = fun
        (Ip) when is_binary(Ip) ->
            {ok, {I1, I2, I3, I4}} = inet:parse_address(binary_to_list(Ip)),
            <<0, 0, I1, I2, I3, I4>>;
        (undefined) ->
            undefined;
        (_) ->
            error(invalid_ip_address)
    end,
    L = [
        maps:get(<<"DialingName">>, Param, undefined),
        maps:get(<<"Username">>, Param, undefined),
        maps:get(<<"Password">>, Param, undefined),
        TuneAdr(maps:get(<<"Ip">>, Param, undefined)),
        TuneWrd(maps:get(<<"Port">>, Param, undefined)),
        TuneBin(maps:get(<<"ManufacturerId">>, Param, undefined), 4),
        TuneBin(maps:get(<<"HardwareVer">>, Param, undefined), 5),
        TuneBin(maps:get(<<"SoftwareVer">>, Param, undefined), 5),
        maps:get(<<"UpgradeUrl">>, Param, undefined),
        TuneWrd(maps:get(<<"Timeout">>, Param, undefined))
    ],
    list_to_binary([I || I <- lists:join(";", L), I /= undefined]).

take_param(K, Params) ->
    V = search_param(K, Params),
    {V, Params -- [#{K => V}]}.

search_param(16#05, [#{16#05 := V} | _]) -> V;
search_param(16#0E, [#{16#0E := V} | _]) -> V;
search_param(K, [_ | Rest]) -> search_param(K, Rest).

cal_check(_, 0, Check) -> Check;
cal_check(<<C:8, Rest/binary>>, Size, undefined) -> cal_check(Rest, Size - 1, C);
cal_check(<<C:8, Rest/binary>>, Size, Check) -> cal_check(Rest, Size - 1, Check bxor C).

format(Msg) ->
    io_lib:format("~p", [Msg]).

type(_) ->
    %% TODO:
    gbt32960.

is_message(#frame{}) ->
    %% TODO:
    true;
is_message(_) ->
    false.
