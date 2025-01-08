%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gbt32960_parser_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_gbt32960.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BYTE, 8 / big - integer).
-define(WORD, 16 / big - integer).
-define(DWORD, 32 / big - integer).
-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

all() ->
    [
        case01_login,
        case02_realtime_report_0x01,
        case03_realtime_report_0x02,
        case04_realtime_report_0x03,
        case05_realtime_report_0x04,
        case06_realtime_report_0x05,
        case07_realtime_report_0x06,
        case08_realtime_report_0x07,
        case09_realtime_report_0x08,
        case10_realtime_report_0x09,
        case11_heartbeat,
        case12_schooltime,
        case13_param_query,
        case14_param_setting,
        case15_terminal_ctrl,
        case16_serialize_ack,
        case17_serialize_query,
        case18_serialize_query,
        case19_serialize_ctrl
    ].

init_per_suite(Config) ->
    emqx_logger:set_log_level(debug),
    Config.

end_per_suite(Config) ->
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% helper functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode(Cmd, Vin, Data) ->
    encode(Cmd, ?ACK_IS_CMD, Vin, ?ENCRYPT_NONE, Data).

encode(Cmd, Ack, Vin, Encrypt, Data) ->
    Size = byte_size(Data),
    S1 = <<Cmd:8, Ack:8, Vin:17/binary, Encrypt:8, Size:16, Data/binary>>,
    Crc = make_crc(S1, undefined),
    Stream = <<"##", S1/binary, Crc:8>>,
    ?LOGT("encode a packet=~p", [binary_to_hex_string(Stream)]),
    Stream.

make_crc(<<>>, Xor) -> Xor;
make_crc(<<C:8, Rest/binary>>, undefined) -> make_crc(Rest, C);
make_crc(<<C:8, Rest/binary>>, Xor) -> make_crc(Rest, C bxor Xor).

make_time() ->
    {Year, Mon, Day} = date(),
    {Hour, Min, Sec} = time(),
    Year1 = list_to_integer(string:substr(integer_to_list(Year), 3, 2)),
    <<Year1:8, Mon:8, Day:8, Hour:8, Min:8, Sec:8>>.

binary_to_hex_string(Data) ->
    lists:flatten([io_lib:format("~2.16.0B ", [X]) || <<X:8>> <= Data]).

to_json(#frame{cmd = Cmd, vin = Vin, encrypt = Encrypt, data = Data}) ->
    emqx_utils_json:encode(#{'Cmd' => Cmd, 'Vin' => Vin, 'Encrypt' => Encrypt, 'Data' => Data}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% test case functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

case01_login(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<12, 12, 29, 12, 19, 20>>,
    Data = <<Time/binary, 1:16, "12345678901234567890", 1, 1, "C">>,
    Bin = encode(?CMD_VIHECLE_LOGIN, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_VIHECLE_LOGIN,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 12,
                <<"Month">> := 12,
                <<"Day">> := 29,
                <<"Hour">> := 12,
                <<"Minute">> := 19,
                <<"Second">> := 20
            },
            <<"Seq">> := 1,
            <<"ICCID">> := <<"12345678901234567890">>,
            <<"Num">> := 1,
            <<"Length">> := 1,
            <<"Id">> := <<"C">>
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case02_realtime_report_0x01(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 1, 1, 2, 59, 0>>,
    VehicleState =
        <<1:?BYTE, 1:?BYTE, 1:?BYTE, 2000:?WORD, 999999:?DWORD, 5000:?WORD, 15000:?WORD, 50:?BYTE,
            1:?BYTE, 5:?BYTE, 6000:?WORD, 90:?BYTE, 0:?BYTE>>,
    Data = <<Time/binary, 16#01, VehicleState/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 1,
                <<"Day">> := 1,
                <<"Hour">> := 2,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Vehicle">>,
                    <<"Status">> := 1,
                    <<"Charging">> := 1,
                    <<"Mode">> := 1,
                    <<"Speed">> := 2000,
                    <<"Mileage">> := 999999,
                    <<"Voltage">> := 5000,
                    <<"Current">> := 15000,
                    <<"SOC">> := 50,
                    <<"DC">> := 1,
                    <<"Gear">> := 5,
                    <<"Resistance">> := 6000,
                    <<"AcceleratorPedal">> := 90,
                    <<"BrakePedal">> := 0
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case03_realtime_report_0x02(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 1, 1, 2, 59, 0>>,
    DriveMotor1 =
        <<1:?BYTE, 1:?BYTE, 125:?BYTE, 30000:?WORD, 25000:?WORD, 125:?BYTE, 30012:?WORD,
            31203:?WORD>>,
    DriveMotor2 =
        <<2:?BYTE, 1:?BYTE, 125:?BYTE, 30200:?WORD, 25300:?WORD, 145:?BYTE, 32000:?WORD,
            30200:?WORD>>,
    Data = <<Time/binary, 16#02, 2:?BYTE, DriveMotor1/binary, DriveMotor2/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 1,
                <<"Day">> := 1,
                <<"Hour">> := 2,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"DriveMotor">>,
                    <<"Number">> := 2,
                    <<"Motors">> := [
                        #{
                            <<"No">> := 1,
                            <<"Status">> := 1,
                            <<"CtrlTemp">> := 125,
                            <<"Rotating">> := 30000,
                            <<"Torque">> := 25000,
                            <<"MotorTemp">> := 125,
                            <<"InputVoltage">> := 30012,
                            <<"DCBusCurrent">> := 31203
                        },
                        #{
                            <<"No">> := 2,
                            <<"Status">> := 1,
                            <<"CtrlTemp">> := 125,
                            <<"Rotating">> := 30200,
                            <<"Torque">> := 25300,
                            <<"MotorTemp">> := 145,
                            <<"InputVoltage">> := 32000,
                            <<"DCBusCurrent">> := 30200
                        }
                    ]
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case04_realtime_report_0x03(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 1, 1, 2, 59, 0>>,
    FuelCell =
        <<10000:?WORD, 12000:?WORD, 45000:?WORD, 2:?WORD, 120:?BYTE, 121:?BYTE, 12500:?WORD,
            10:?BYTE, 35000:?WORD, 11:?BYTE, 500:?WORD, 12:?BYTE, 1:?BYTE>>,
    Data = <<Time/binary, 16#03, FuelCell/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 1,
                <<"Day">> := 1,
                <<"Hour">> := 2,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"FuelCell">>,
                    <<"CellVoltage">> := 10000,
                    <<"CellCurrent">> := 12000,
                    <<"FuelConsumption">> := 45000,
                    <<"ProbeNum">> := 2,
                    <<"ProbeTemps">> := [120, 121],
                    <<"H_MaxTemp">> := 12500,
                    <<"H_TempProbeCode">> := 10,
                    <<"H_MaxConc">> := 35000,
                    <<"H_ConcSensorCode">> := 11,
                    <<"H_MaxPress">> := 500,
                    <<"H_PressSensorCode">> := 12,
                    <<"DCStatus">> := 1
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case05_realtime_report_0x04(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 10, 1, 22, 59, 0>>,
    Data = <<Time/binary, 16#04, 16#01, 2000:?WORD, 200:?WORD>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 10,
                <<"Day">> := 1,
                <<"Hour">> := 22,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Engine">>,
                    <<"Status">> := 1,
                    <<"CrankshaftSpeed">> := 2000,
                    <<"FuelConsumption">> := 200
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case06_realtime_report_0x05(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 10, 1, 22, 59, 0>>,
    Data = <<Time/binary, 16#05, 16#00, 10:?DWORD, 100:?DWORD>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 10,
                <<"Day">> := 1,
                <<"Hour">> := 22,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Location">>,
                    <<"Status">> := 0,
                    <<"Longitude">> := 10,
                    <<"Latitude">> := 100
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case07_realtime_report_0x06(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<17, 5, 30, 12, 22, 59>>,
    Extreme =
        <<12:?BYTE, 10:?BYTE, 7500:?WORD, 13:?BYTE, 11:?BYTE, 2000:?WORD, 14:?BYTE, 12:?BYTE,
            120:?BYTE, 15:?BYTE, 13:?BYTE, 40:?BYTE>>,
    Data = <<Time/binary, 16#06, Extreme/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 5,
                <<"Day">> := 30,
                <<"Hour">> := 12,
                <<"Minute">> := 22,
                <<"Second">> := 59
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Extreme">>,
                    <<"MaxVoltageBatterySubsysNo">> := 12,
                    <<"MaxVoltageBatteryCode">> := 10,
                    <<"MaxBatteryVoltage">> := 7500,
                    <<"MinVoltageBatterySubsysNo">> := 13,
                    <<"MinVoltageBatteryCode">> := 11,
                    <<"MinBatteryVoltage">> := 2000,
                    <<"MaxTempSubsysNo">> := 14,
                    <<"MaxTempProbeNo">> := 12,
                    <<"MaxTemp">> := 120,
                    <<"MinTempSubsysNo">> := 15,
                    <<"MinTempProbeNo">> := 13,
                    <<"MinTemp">> := 40
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case08_realtime_report_0x07(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<17, 12, 20, 22, 23, 59>>,
    Alarm =
        <<2:?BYTE, 0:?DWORD, 1:?BYTE, 123:?DWORD, 2:?BYTE, 123:?DWORD, 223:?DWORD, 1:?BYTE,
            123:?DWORD, 1:?BYTE, 125:?DWORD>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, <<Time/binary, 16#07, Alarm/binary>>),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 20,
                <<"Hour">> := 22,
                <<"Minute">> := 23,
                <<"Second">> := 59
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Alarm">>,
                    <<"MaxAlarmLevel">> := 2,
                    <<"GeneralAlarmFlag">> := 0,
                    <<"FaultChargeableDeviceNum">> := 1,
                    <<"FaultChargeableDeviceList">> := [<<"007B">>],
                    <<"FaultDriveMotorNum">> := 2,
                    <<"FaultDriveMotorList">> := [<<"007B">>, <<"00DF">>],
                    <<"FaultEngineNum">> := 1,
                    <<"FaultEngineList">> := [<<"007B">>],
                    <<"FaultOthersNum">> := 1,
                    <<"FaultOthersList">> := [<<"007D">>]
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),

    Alarm1 = <<1:?BYTE, 3:?DWORD, 1:?BYTE, 200:?DWORD, 0:?BYTE, 1:?BYTE, 111:?DWORD, 0:?BYTE>>,
    Bin1 = encode(
        ?CMD_INFO_RE_REPORT, <<"1G1BL52P7TR115520">>, <<Time/binary, 16#07, Alarm1/binary>>
    ),
    {ok, Frame1, <<>>, _State1} = emqx_gbt32960_frame:parse(Bin1, Parser),
    #frame{
        cmd = ?CMD_INFO_RE_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 20,
                <<"Hour">> := 22,
                <<"Minute">> := 23,
                <<"Second">> := 59
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"Alarm">>,
                    <<"MaxAlarmLevel">> := 1,
                    <<"GeneralAlarmFlag">> := 3,
                    <<"FaultChargeableDeviceNum">> := 1,
                    <<"FaultChargeableDeviceList">> := [<<"00C8">>],
                    <<"FaultDriveMotorNum">> := 0,
                    <<"FaultDriveMotorList">> := [],
                    <<"FaultEngineNum">> := 1,
                    <<"FaultEngineList">> := [<<"006F">>],
                    <<"FaultOthersNum">> := 0,
                    <<"FaultOthersList">> := []
                }
            ]
        }
    } = Frame1,
    ?LOGT("frame: ~p", [to_json(Frame1)]),
    ok.

case09_realtime_report_0x08(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 10, 1, 22, 59, 0>>,
    VoltageSys1 = <<1:?BYTE, 5000:?WORD, 10000:?WORD, 2:?WORD, 0:?WORD, 1:?BYTE, 5000:?WORD>>,
    VoltageSys2 = <<2:?BYTE, 5001:?WORD, 10001:?WORD, 2:?WORD, 1:?WORD, 1:?BYTE, 5001:?WORD>>,
    Data = <<Time/binary, 16#08, 16#02, VoltageSys1/binary, VoltageSys2/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 10,
                <<"Day">> := 1,
                <<"Hour">> := 22,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"ChargeableVoltage">>,
                    <<"Number">> := 2,
                    <<"SubSystems">> := [
                        #{
                            <<"ChargeableSubsysNo">> := 1,
                            <<"ChargeableVoltage">> := 5000,
                            <<"ChargeableCurrent">> := 10000,
                            <<"CellsTotal">> := 2,
                            <<"FrameCellsIndex">> := 0,
                            <<"FrameCellsCount">> := 1,
                            <<"CellsVoltage">> := [5000]
                        },
                        #{
                            <<"ChargeableSubsysNo">> := 2,
                            <<"ChargeableVoltage">> := 5001,
                            <<"ChargeableCurrent">> := 10001,
                            <<"CellsTotal">> := 2,
                            <<"FrameCellsIndex">> := 1,
                            <<"FrameCellsCount">> := 1,
                            <<"CellsVoltage">> := [5001]
                        }
                    ]
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case10_realtime_report_0x09(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 10, 1, 22, 59, 0>>,
    Temp1 = <<1:?BYTE, 10:?WORD, 5000:80>>,
    Temp2 = <<2:?BYTE, 1:?WORD, 100:?BYTE>>,
    Data = <<Time/binary, 16#09, 16#02, Temp1/binary, Temp2/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 10,
                <<"Day">> := 1,
                <<"Hour">> := 22,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"ChargeableTemp">>,
                    <<"Number">> := 2,
                    <<"SubSystems">> := [
                        #{
                            <<"ChargeableSubsysNo">> := 1,
                            <<"ProbeNum">> := 10,
                            <<"ProbesTemp">> := [0, 0, 0, 0, 0, 0, 0, 0, 19, 136]
                        },
                        #{
                            <<"ChargeableSubsysNo">> := 2,
                            <<"ProbeNum">> := 1,
                            <<"ProbesTemp">> := [100]
                        }
                    ]
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case11_heartbeat(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Bin = encode(?CMD_HEARTBEAT, <<"1G1BL52P7TR115520">>, <<>>),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_HEARTBEAT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{}
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case12_schooltime(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Bin = encode(?CMD_SCHOOL_TIME, <<"1G1BL52P7TR115520">>, <<>>),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_SCHOOL_TIME,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{}
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case13_param_query(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<17, 12, 18, 9, 22, 30>>,
    Data =
        <<Time/binary, 5, 1, 5000:?WORD, 4, 10, 5, "google.com", 16#0D, 14, 16#0E,
            "www.google.com">>,
    Bin = encode(?CMD_PARAM_QUERY, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_PARAM_QUERY,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 18,
                <<"Hour">> := 9,
                <<"Minute">> := 22,
                <<"Second">> := 30
            },
            <<"Total">> := 5,
            <<"Params">> := [
                #{<<"0x01">> := 5000},
                #{<<"0x04">> := 10},
                #{<<"0x05">> := <<"google.com">>},
                #{<<"0x0D">> := 14},
                #{<<"0x0E">> := <<"www.google.com">>}
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case14_param_setting(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<17, 12, 18, 9, 22, 30>>,
    Data =
        <<Time/binary, 5, 1, 5000:?WORD, 4, 10, 5, "google.com", 16#0D, 14, 16#0E,
            "www.google.com">>,
    Bin = encode(?CMD_PARAM_SETTING, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_PARAM_SETTING,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 18,
                <<"Hour">> := 9,
                <<"Minute">> := 22,
                <<"Second">> := 30
            },
            <<"Total">> := 5,
            <<"Params">> := [
                #{<<"0x01">> := 5000},
                #{<<"0x04">> := 10},
                #{<<"0x05">> := <<"google.com">>},
                #{<<"0x0D">> := 14},
                #{<<"0x0E">> := <<"www.google.com">>}
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.

case15_terminal_ctrl(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<17, 12, 18, 9, 22, 30>>,
    Data = <<Time/binary, 16#02>>,
    Bin = encode(?CMD_TERMINAL_CTRL, <<"1G1BL52P7TR115520">>, Data),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 18,
                <<"Hour">> := 9,
                <<"Minute">> := 22,
                <<"Second">> := 30
            },
            <<"Command">> := 2,
            <<"Param">> := <<>>
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),

    Param1 =
        <<"emqtt;eusername;password;", 0, 0, 192, 168, 1, 1, ";", 8080:?WORD,
            ";vhid;1.0.0;0.0.1;ftp://emqtt.io/ftp/server;", 3000:?WORD>>,
    Data1 = <<Time/binary, 16#01, Param1/binary>>,
    Bin1 = encode(?CMD_TERMINAL_CTRL, <<"1G1BL52P7TR115520">>, Data1),
    {ok, Frame1, <<>>, _State1} = emqx_gbt32960_frame:parse(Bin1, Parser),
    #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 18,
                <<"Hour">> := 9,
                <<"Minute">> := 22,
                <<"Second">> := 30
            },
            <<"Command">> := 1,
            <<"Param">> := #{
                <<"DialingName">> := <<"emqtt">>,
                <<"Username">> := <<"eusername">>,
                <<"Password">> := <<"password">>,
                <<"Ip">> := <<"192.168.1.1">>,
                <<"Port">> := 8080,
                <<"ManufacturerId">> := <<"vhid">>,
                <<"HardwareVer">> := <<"1.0.0">>,
                <<"SoftwareVer">> := <<"0.0.1">>,
                <<"UpgradeUrl">> := <<"ftp://emqtt.io/ftp/server">>,
                <<"Timeout">> := 3000
            }
        }
    } = Frame1,
    ?LOGT("frame: ~p", [to_json(Frame1)]),

    Param2 = <<"This is a alarm text!!!">>,
    Data2 = <<Time/binary, 16#06, 16#01, Param2/binary>>,
    Bin2 = encode(?CMD_TERMINAL_CTRL, <<"1G1BL52P7TR115520">>, Data2),
    {ok, Frame2, <<>>, _State2} = emqx_gbt32960_frame:parse(Bin2, Parser),
    #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 12,
                <<"Day">> := 18,
                <<"Hour">> := 9,
                <<"Minute">> := 22,
                <<"Second">> := 30
            },
            <<"Command">> := 6,
            <<"Param">> := #{
                <<"Level">> := 1,
                <<"Message">> := Param2
            }
        }
    } = Frame2,
    ?LOGT("frame: ~p", [to_json(Frame2)]),
    ok.

case16_serialize_ack(_Config) ->
    % Vechile login
    DataUnit = <<1, 1, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1>>,
    Frame = #frame{
        cmd = ?CMD_VIHECLE_LOGIN,
        ack = ?ACK_SUCCESS,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 11,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 20,
                <<"Minute">> => 5,
                <<"Second">> => 51
            }
        },
        rawdata = <<17, 11, 23, 21, 4, 50, DataUnit/binary>>
    },
    Bin = emqx_gbt32960_frame:serialize(Frame),
    BodyLen = byte_size(Bin) - 3,
    <<"##", Body:BodyLen/binary, Crc:?BYTE>> = Bin,
    <<?CMD_VIHECLE_LOGIN, ?ACK_SUCCESS, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 26:?WORD, 11:?BYTE,
        10:?BYTE, 25:?BYTE, 20:?BYTE, 5:?BYTE, 51:?BYTE, DataUnit/binary>> = Body,
    Crc = make_crc(Body, undefined),
    ok.

case17_serialize_query(_Config) ->
    DataUnit = <<2, 1, 2>>,
    Frame = #frame{
        cmd = ?CMD_PARAM_QUERY,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 11,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 20,
                <<"Minute">> => 5,
                <<"Second">> => 51
            },
            <<"Total">> => 2,
            <<"Ids">> => [1, 2]
        }
    },
    Bin = emqx_gbt32960_frame:serialize(Frame),
    BodyLen = byte_size(Bin) - 3,
    <<"##", Body:BodyLen/binary, Crc:?BYTE>> = Bin,
    <<?CMD_PARAM_QUERY, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 9:?WORD, 11, 10, 25, 20, 5,
        51, DataUnit/binary>> = Body,
    Crc = make_crc(Body, undefined),
    ok.

case18_serialize_query(_Config) ->
    DataUnit =
        <<6, 1, 30000:?WORD, 4, 10, 5, "google.com", 7, "1.0.0", 16#0D, 14, 16#0E,
            "www.google.com">>,
    Frame = #frame{
        cmd = ?CMD_PARAM_SETTING,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 17,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 23,
                <<"Minute">> => 59,
                <<"Second">> => 59
            },
            <<"Total">> => 6,
            <<"Params">> => [
                #{1 => 30000},
                #{4 => 10},
                #{5 => <<"google.com">>},
                #{7 => <<"1.0.0">>},
                #{16#0D => 14},
                #{16#0E => <<"www.google.com">>}
            ]
        }
    },
    Bin = emqx_gbt32960_frame:serialize(Frame),
    BodyLen = byte_size(Bin) - 3,
    <<"##", Body:BodyLen/binary, Crc:?BYTE>> = Bin,
    <<?CMD_PARAM_SETTING, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 46:?WORD, 17, 10, 25, 23,
        59, 59, DataUnit/binary>> = Body,
    Crc = make_crc(Body, undefined),
    ok.

case19_serialize_ctrl(_Config) ->
    Frame = #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 17,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 22,
                <<"Minute">> => 5,
                <<"Second">> => 51
            },
            <<"Command">> => 2,
            <<"Param">> => <<>>
        }
    },
    Bin = emqx_gbt32960_frame:serialize(Frame),
    BodyLen = byte_size(Bin) - 3,
    <<"##", Body:BodyLen/binary, Crc:?BYTE>> = Bin,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 7:?WORD, 17, 10, 25, 22,
        5, 51, 2>> = Body,
    Crc = make_crc(Body, undefined),

    DataUnit1 = <<"The alarm has occurred!">>,
    Frame1 = #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 17,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 22,
                <<"Minute">> => 5,
                <<"Second">> => 51
            },
            <<"Command">> => 6,
            <<"Param">> => #{
                <<"Level">> => 1,
                <<"Message">> => DataUnit1
            }
        }
    },
    Bin1 = emqx_gbt32960_frame:serialize(Frame1),
    BodyLen1 = byte_size(Bin1) - 3,
    <<"##", Body1:BodyLen1/binary, Crc1:?BYTE>> = Bin1,
    EncryptedLen1 = 31,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, EncryptedLen1:?WORD, 17,
        10, 25, 22, 5, 51, 6, 1, DataUnit1/binary>> = Body1,
    Crc1 = make_crc(Body1, undefined),

    DataUnit2 =
        <<"emqtt;eusername;password;", 0, 0, 192, 168, 1, 1, ";", 8080:?WORD,
            ";BWM1;1.0.0;0.0.1;ftp://emqtt.io/ftp/server;", 3000:?WORD>>,
    Frame2 = #frame{
        cmd = ?CMD_TERMINAL_CTRL,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> => #{
                <<"Year">> => 17,
                <<"Month">> => 10,
                <<"Day">> => 25,
                <<"Hour">> => 22,
                <<"Minute">> => 5,
                <<"Second">> => 51
            },
            <<"Command">> => 1,
            <<"Param">> => #{
                <<"DialingName">> => <<"emqtt">>,
                <<"Username">> => <<"eusername">>,
                <<"Password">> => <<"password">>,
                <<"Ip">> => <<"192.168.1.1">>,
                <<"Port">> => 8080,
                <<"ManufacturerId">> => <<"BWM1">>,
                <<"HardwareVer">> => <<"1.0.0">>,
                <<"SoftwareVer">> => <<"0.0.1">>,
                <<"UpgradeUrl">> => <<"ftp://emqtt.io/ftp/server">>,
                <<"Timeout">> => 3000
            }
        }
    },
    Bin2 = emqx_gbt32960_frame:serialize(Frame2),
    BodyLen2 = byte_size(Bin2) - 3,
    <<"##", Body2:BodyLen2/binary, Crc2:?BYTE>> = Bin2,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 87:?WORD, 17, 10, 25, 22,
        5, 51, 1, DataUnitSeried2/binary>> = Body2,
    ?assertEqual(DataUnit2, DataUnitSeried2),
    ?assertEqual(Crc2, make_crc(Body2, undefined)),
    ok.

case20_realtime_report_0x80(_Config) ->
    Parser = emqx_gbt32960_frame:initial_parse_state(#{}),
    Time = <<16, 10, 1, 22, 59, 0>>,
    InfoData = <<16#80, 16#02, "0000000">>,
    Data = <<Time/binary, InfoData/binary>>,
    Bin = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    Base64Data = base64:encode(InfoData),
    {ok, Frame, <<>>, _State} = emqx_gbt32960_frame:parse(Bin, Parser),
    #frame{
        cmd = ?CMD_INFO_REPORT,
        ack = ?ACK_IS_CMD,
        vin = <<"1G1BL52P7TR115520">>,
        encrypt = ?ENCRYPT_NONE,
        data = #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 10,
                <<"Day">> := 1,
                <<"Hour">> := 22,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Infos">> := [
                #{
                    <<"Type">> := <<"CustomData">>,
                    <<"Data">> := Base64Data
                }
            ]
        }
    } = Frame,
    ?LOGT("frame: ~p", [to_json(Frame)]),
    ok.
