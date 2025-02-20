%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(FLAG, 1 / binary).
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
    phase := phase()
}.

-ifdef(TEST).
-export([serialize/1]).
-endif.

%%--------------------------------------------------------------------
%% Init a Parser
%%--------------------------------------------------------------------

-spec initial_parse_state(map()) -> parser_state().
initial_parse_state(_) ->
    #{data => <<>>, phase => search_heading}.

-spec serialize_opts() -> emqx_gateway_frame:serialize_options().
serialize_opts() ->
    #{}.

%%--------------------------------------------------------------------
%% Parse Message
%%--------------------------------------------------------------------
parse(Bin, State) ->
    case enter_parse(Bin, State) of
        {ok, Message, Rest} ->
            {ok, Message, Rest, State#{data => <<>>, phase => search_heading}};
        {error, Error} ->
            {error, Error};
        {more_data_follow, Partial} ->
            {more, State#{data => Partial, phase => parse}}
    end.

enter_parse(Bin, #{phase := search_heading}) ->
    case search_heading(Bin) of
        {ok, Rest} ->
            parse_msg(Rest);
        Error ->
            Error
    end;
enter_parse(Bin, #{data := Data}) ->
    parse_msg(<<Data/binary, Bin/binary>>).

search_heading(<<16#23, 16#23, Rest/binary>>) ->
    {ok, Rest};
search_heading(<<_, Rest/binary>>) ->
    search_heading(Rest);
search_heading(<<>>) ->
    {error, invalid_frame}.

parse_msg(Binary) ->
    case byte_size(Binary) >= ?HEADER_SIZE of
        true ->
            {Frame, Rest2} = parse_header(Binary),
            case byte_size(Rest2) >= Frame#frame.length + 1 of
                true -> parse_body(Rest2, Frame);
                false -> {more_data_follow, Binary}
            end;
        false ->
            {more_data_follow, Binary}
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
    #frame{cmd = ?CMD_VIHECLE_LOGIN},
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
    #frame{cmd = ?CMD_INFO_REPORT},
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
        <<"Infos">> => parse_info(Infos, [])
    };
parse_data(
    #frame{cmd = ?CMD_INFO_RE_REPORT},
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
        <<"Infos">> => parse_info(Infos, [])
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
    #frame{cmd = ?CMD_PARAM_QUERY},
    <<Year:?BYTE, Month:?BYTE, Day:?BYTE, Hour:?BYTE, Minute:?BYTE, Second:?BYTE, Total:?BYTE,
        Rest/binary>>
) ->
    %% XXX: need check ACK field?
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
        <<"Params">> => parse_params(Rest)
    };
parse_data(
    #frame{cmd = ?CMD_PARAM_SETTING},
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
        <<"Params">> => parse_params(Rest)
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
parse_data(Frame, Data) ->
    ?SLOG(error, #{msg => "invalid_frame", frame => Frame, data => Data}),
    error(invalid_frame).

%%--------------------------------------------------------------------
%% Parse Report Data Info
%%--------------------------------------------------------------------

parse_info(<<>>, Acc) ->
    lists:reverse(Acc);
parse_info(<<?INFO_TYPE_VEHICLE, Body:20/binary, Rest/binary>>, Acc) ->
    <<Status:?BYTE, Charging:?BYTE, Mode:?BYTE, Speed:?WORD, Mileage:?DWORD, Voltage:?WORD,
        Current:?WORD, SOC:?BYTE, DC:?BYTE, Gear:?BYTE, Resistance:?WORD, AcceleratorPedal:?BYTE,
        BrakePedal:?BYTE>> = Body,
    parse_info(Rest, [
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
    ]);
parse_info(<<?INFO_TYPE_DRIVE_MOTOR, Number, Rest/binary>>, Acc) ->
    % 12 is packet len of per drive motor
    Len = Number * 12,
    <<Bodys:Len/binary, Rest1/binary>> = Rest,
    parse_info(Rest1, [
        #{
            <<"Type">> => <<"DriveMotor">>,
            <<"Number">> => Number,
            <<"Motors">> => parse_drive_motor(Bodys, [])
        }
        | Acc
    ]);
parse_info(<<?INFO_TYPE_FUEL_CELL, Rest/binary>>, Acc) ->
    <<CellVoltage:?WORD, CellCurrent:?WORD, FuelConsumption:?WORD, ProbeNum:?WORD, Rest1/binary>> =
        Rest,

    <<ProbeTemps:ProbeNum/binary, Rest2/binary>> = Rest1,

    <<HMaxTemp:?WORD, HTempProbeCode:?BYTE, HMaxConc:?WORD, HConcSensorCode:?BYTE, HMaxPress:?WORD,
        HPressSensorCode:?BYTE, DCStatus:?BYTE, Rest3/binary>> = Rest2,
    parse_info(Rest3, [
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
    ]);
parse_info(
    <<?INFO_TYPE_ENGINE, Status:?BYTE, CrankshaftSpeed:?WORD, FuelConsumption:?WORD, Rest/binary>>,
    Acc
) ->
    parse_info(Rest, [
        #{
            <<"Type">> => <<"Engine">>,
            <<"Status">> => Status,
            <<"CrankshaftSpeed">> => CrankshaftSpeed,
            <<"FuelConsumption">> => FuelConsumption
        }
        | Acc
    ]);
parse_info(
    <<?INFO_TYPE_LOCATION, Status:?BYTE, Longitude:?DWORD, Latitude:?DWORD, Rest/binary>>, Acc
) ->
    parse_info(Rest, [
        #{
            <<"Type">> => <<"Location">>,
            <<"Status">> => Status,
            <<"Longitude">> => Longitude,
            <<"Latitude">> => Latitude
        }
        | Acc
    ]);
parse_info(<<?INFO_TYPE_EXTREME, Body:14/binary, Rest/binary>>, Acc) ->
    <<MaxVoltageBatterySubsysNo:?BYTE, MaxVoltageBatteryCode:?BYTE, MaxBatteryVoltage:?WORD,
        MinVoltageBatterySubsysNo:?BYTE, MinVoltageBatteryCode:?BYTE, MinBatteryVoltage:?WORD,
        MaxTempSubsysNo:?BYTE, MaxTempProbeNo:?BYTE, MaxTemp:?BYTE, MinTempSubsysNo:?BYTE,
        MinTempProbeNo:?BYTE, MinTemp:?BYTE>> = Body,

    parse_info(Rest, [
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
    ]);
parse_info(<<?INFO_TYPE_ALARM, Rest/binary>>, Acc) ->
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
    parse_info(Rest5, [
        #{
            <<"Type">> => <<"Alarm">>,
            <<"MaxAlarmLevel">> => MaxAlarmLevel,
            <<"GeneralAlarmFlag">> => GeneralAlarmFlag,
            <<"FaultChargeableDeviceNum">> => FaultChargeableDeviceNum,
            <<"FaultChargeableDeviceList">> => tune_fault_codelist(FaultChargeableDeviceList),
            <<"FaultDriveMotorNum">> => FaultDriveMotorNum,
            <<"FaultDriveMotorList">> => tune_fault_codelist(FaultDriveMotorList),
            <<"FaultEngineNum">> => FaultEngineNum,
            <<"FaultEngineList">> => tune_fault_codelist(FaultEngineList),
            <<"FaultOthersNum">> => FaultOthersNum,
            <<"FaultOthersList">> => tune_fault_codelist(FaultOthersList)
        }
        | Acc
    ]);
parse_info(<<?INFO_TYPE_CHARGEABLE_VOLTAGE, Number:?BYTE, Rest/binary>>, Acc) ->
    {Rest1, SubSystems} = parse_chargeable_voltage(Rest, Number, []),
    parse_info(Rest1, [
        #{
            <<"Type">> => <<"ChargeableVoltage">>,
            <<"Number">> => Number,
            <<"SubSystems">> => SubSystems
        }
        | Acc
    ]);
parse_info(<<?INFO_TYPE_CHARGEABLE_TEMP, Number:?BYTE, Rest/binary>>, Acc) ->
    {Rest1, SubSystems} = parse_chargeable_temp(Rest, Number, []),
    parse_info(Rest1, [
        #{
            <<"Type">> => <<"ChargeableTemp">>,
            <<"Number">> => Number,
            <<"SubSystems">> => SubSystems
        }
        | Acc
    ]);
parse_info(Rest, Acc) ->
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

parse_drive_motor(<<>>, Acc) ->
    lists:reverse(Acc);
parse_drive_motor(
    <<No:?BYTE, Status:?BYTE, CtrlTemp:?BYTE, Rotating:?WORD, Torque:?WORD, MotorTemp:?BYTE,
        InputVoltage:?WORD, DCBusCurrent:?WORD, Rest/binary>>,
    Acc
) ->
    parse_drive_motor(Rest, [
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
    ]).

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
tune_fault_codelist(<<>>) ->
    [];
tune_fault_codelist(Data) ->
    lists:flatten([list_to_binary(io_lib:format("~4.16.0B", [X])) || <<X:?DWORD>> <= Data]).

tune_voltage(Bin) -> tune_voltage_(Bin, []).
tune_voltage_(<<>>, Acc) -> lists:reverse(Acc);
tune_voltage_(<<V:?WORD, Rest/binary>>, Acc) -> tune_voltage_(Rest, [V | Acc]).

parse_params(Bin) -> parse_params_(Bin, []).
parse_params_(<<>>, Acc) ->
    lists:reverse(Acc);
parse_params_(<<16#01, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x01">> => Val} | Acc]);
parse_params_(<<16#02, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x02">> => Val} | Acc]);
parse_params_(<<16#03, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x03">> => Val} | Acc]);
parse_params_(<<16#04, Val:?BYTE, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x04">> => Val} | Acc]);
parse_params_(<<16#05, Rest/binary>>, Acc) ->
    case [V || #{<<"0x04">> := V} <- Acc] of
        [Len] ->
            <<Val:Len/binary, Rest1/binary>> = Rest,
            parse_params_(Rest1, [#{<<"0x05">> => Val} | Acc]);
        _ ->
            ?SLOG(error, #{
                msg => "invalid_data", reason => "cmd_0x04 must appear ahead of cmd_0x05"
            }),
            lists:reverse(Acc)
    end;
parse_params_(<<16#06, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x06">> => Val} | Acc]);
parse_params_(<<16#07, Val:5/binary, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x07">> => Val} | Acc]);
parse_params_(<<16#08, Val:5/binary, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x08">> => Val} | Acc]);
parse_params_(<<16#09, Val:?BYTE, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x09">> => Val} | Acc]);
parse_params_(<<16#0A, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x0A">> => Val} | Acc]);
parse_params_(<<16#0B, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x0B">> => Val} | Acc]);
parse_params_(<<16#0C, Val:?BYTE, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x0C">> => Val} | Acc]);
parse_params_(<<16#0D, Val:?BYTE, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x0D">> => Val} | Acc]);
parse_params_(<<16#0E, Rest/binary>>, Acc) ->
    case [V || #{<<"0x0D">> := V} <- Acc] of
        [Len] ->
            <<Val:Len/binary, Rest1/binary>> = Rest,
            parse_params_(Rest1, [#{<<"0x0E">> => Val} | Acc]);
        _ ->
            ?SLOG(error, #{
                msg => "invalid_data", reason => "cmd_0x0D must appear ahead of cmd_0x0E"
            }),
            lists:reverse(Acc)
    end;
parse_params_(<<16#0F, Val:?WORD, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x0F">> => Val} | Acc]);
parse_params_(<<16#10, Val:?BYTE, Rest/binary>>, Acc) ->
    parse_params_(Rest, [#{<<"0x10">> => Val} | Acc]);
parse_params_(Cmd, Acc) ->
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

serialize(#frame{cmd = Cmd, ack = Ack, vin = Vin, encrypt = Encrypt, data = Data, rawdata = RawData}) ->
    Encrypted = encipher(serialize_data(Cmd, Ack, RawData, Data), Encrypt),
    Len = byte_size(Encrypted),
    Stream = <<Cmd:?BYTE, Ack:?BYTE, Vin:17/binary, Encrypt:?BYTE, Len:?WORD, Encrypted/binary>>,
    Crc = cal_check(Stream, byte_size(Stream), undefined),
    <<"##", Stream/binary, Crc:?BYTE>>.

serialize_data(?CMD_PARAM_QUERY, ?ACK_IS_CMD, _, #{
    <<"Time">> := Time,
    <<"Total">> := Total,
    <<"Ids">> := Ids
}) when length(Ids) == Total ->
    T = tune_time(Time),
    Ids1 = tune_ids(Ids),
    <<T/binary, Total:?BYTE, Ids1/binary>>;
serialize_data(?CMD_PARAM_SETTING, ?ACK_IS_CMD, _, #{
    <<"Time">> := Time,
    <<"Total">> := Total,
    <<"Params">> := Params
}) when length(Params) == Total ->
    T = tune_time(Time),
    Params1 = tune_params(Params),
    <<T/binary, Total:?BYTE, Params1/binary>>;
serialize_data(?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, _, #{
    <<"Time">> := Time,
    <<"Command">> := Cmd,
    <<"Param">> := Param
}) ->
    T = tune_time(Time),
    Param1 = tune_ctrl_param(Cmd, Param),
    <<T/binary, Cmd:?BYTE, Param1/binary>>;
serialize_data(_Cmd, Ack, RawData, #{<<"Time">> := Time}) when ?IS_RESPONSE(Ack) ->
    Rest =
        case byte_size(RawData) > 6 of
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

tune_params(Params) ->
    tune_params_(lists:reverse(Params), <<>>).

tune_params_([], Bin) ->
    Bin;
tune_params_([#{16#01 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#01:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#02 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#02:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#03 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#03:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#04 := Val} | Rest], Bin) ->
    {Val05, Rest1} = take_param(16#05, Rest),
    tune_params_(Rest1, <<16#04:?BYTE, Val:?BYTE, 16#05, Val05:Val/binary, Bin/binary>>);
tune_params_([#{16#05 := Val} | Rest], Bin) ->
    tune_params_(Rest ++ [#{16#05 => Val}], Bin);
tune_params_([#{16#06 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#06:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#07 := Val} | Rest], Bin) when byte_size(Val) == 5 ->
    tune_params_(Rest, <<16#07:?BYTE, Val/binary, Bin/binary>>);
tune_params_([#{16#08 := Val} | Rest], Bin) when byte_size(Val) == 5 ->
    tune_params_(Rest, <<16#08:?BYTE, Val/binary, Bin/binary>>);
tune_params_([#{16#09 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#09:?BYTE, Val:?BYTE, Bin/binary>>);
tune_params_([#{16#0A := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#0A:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#0B := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#0B:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#0C := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#0C:?BYTE, Val:?BYTE, Bin/binary>>);
tune_params_([#{16#0D := Val} | Rest], Bin) ->
    {Val0E, Rest1} = take_param(16#0E, Rest),
    tune_params_(Rest1, <<16#0D:?BYTE, Val:?BYTE, 16#0E, Val0E:Val/binary, Bin/binary>>);
tune_params_([#{16#0E := Val} | Rest], Bin) ->
    tune_params_(Rest ++ [#{16#0E => Val}], Bin);
tune_params_([#{16#0F := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#0F:?BYTE, Val:?WORD, Bin/binary>>);
tune_params_([#{16#10 := Val} | Rest], Bin) ->
    tune_params_(Rest, <<16#10:?BYTE, Val:?BYTE, Bin/binary>>).

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
