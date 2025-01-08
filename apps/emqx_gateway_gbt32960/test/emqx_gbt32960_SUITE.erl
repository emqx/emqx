%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gbt32960_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_gbt32960.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(BYTE, 8 / big - integer).
-define(WORD, 16 / big - integer).
-define(DWORD, 32 / big - integer).

-define(PORT, 7325).
-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.gbt32960 {\n"
    " retry_interval = \"1s\"\n"
    " listeners.tcp.default {\n"
    "    bind = 7325\n"
    "  }\n"
    "}\n"
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_gbt32960),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _Config) ->
    snabbkaffe:stop(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% helper functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode(Cmd, Vin, Data) ->
    encode(Cmd, ?ACK_IS_CMD, Vin, ?ENCRYPT_NONE, Data).

encode(Cmd, Ack, Vin, Data) ->
    encode(Cmd, Ack, Vin, ?ENCRYPT_NONE, Data).

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

get_published_msg() ->
    receive
        {deliver, _Topic, #message{topic = Topic, payload = Payload}} ->
            {Topic, Payload}
    after 5000 ->
        error(timeout)
    end.

get_subscriptions() ->
    lists:map(fun({_, Topic}) -> Topic end, ets:tab2list(emqx_subscription)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%% test cases %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
login_first() ->
    emqx:subscribe("gbt32960/+/upstream/#"),

    %
    % send VEHICLE LOGIN
    %
    Time = <<12, 12, 29, 12, 19, 20>>,
    Data = <<Time/binary, 1:?WORD, "12345678901234567890", 1, 1, "C">>,
    Packet = encode(?CMD_VIHECLE_LOGIN, <<"1G1BL52P7TR115520">>, Data),

    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {ok, AckPacket} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("ack packet: ~p", [binary_to_hex_string(AckPacket)]),

    BodyLen = byte_size(AckPacket) - 3,
    <<"##", Body:BodyLen/binary, Crc:8>> = AckPacket,
    <<?CMD_VIHECLE_LOGIN, ?ACK_SUCCESS, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 31:?WORD,
        Time2:6/binary, 1:?WORD, "12345678901234567890", 1, 1, "C">> = Body,

    % has subscribe dnstream topic
    ?assertNotEqual(Time, Time2),
    ?assertEqual(Crc, make_crc(Body, undefined)),

    ?assertEqual(
        true, lists:member(<<"gbt32960/1G1BL52P7TR115520/dnstream">>, get_subscriptions())
    ),

    % check has publish connected message
    {<<"gbt32960/1G1BL52P7TR115520/upstream/vlogin">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_VIHECLE_LOGIN,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    % vehicle login success
    Time = <<12, 12, 29, 12, 19, 20>>,
    {ok, Socket}.

t_case01_login(_Config) ->
    emqx_gateway_test_utils:meck_emqx_hook_calls(),
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    ?assertMatch(
        ['client.connect' | _],
        emqx_gateway_test_utils:collect_emqx_hooks_calls()
    ),

    ok = gen_tcp:close(Socket).

t_case01_login_channel_info(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),
    Vin = <<"1G1BL52P7TR115520">>,
    DnstreamTopic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    %% assert: client info is stored in emqx_gateway_cm

    ?assertMatch(#{clientinfo := #{clientid := Vin}}, emqx_gateway_cm:get_chan_info(gbt32960, Vin)),
    %% assert: suboption has been added
    ?assertEqual(
        true, lists:member(DnstreamTopic, get_subscriptions())
    ),

    ok = gen_tcp:close(Socket).

t_case01_auth_expire(_Config) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
        end
    ),

    ?assertWaitEvent(
        begin
            {ok, _Socket} = login_first()
        end,
        #{
            ?snk_kind := conn_process_terminated,
            clientid := <<"1G1BL52P7TR115520">>,
            reason := {shutdown, expired}
        },
        5000
    ).

t_case02_reportinfo_0x01(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    VehicleState =
        <<1:?BYTE, 1:?BYTE, 1:?BYTE, 2000:?WORD, 999999:?DWORD, 5000:?WORD, 15000:?WORD, 50:?BYTE,
            1:?BYTE, 5:?BYTE, 6000:?WORD, 90:?BYTE, 0:?BYTE>>,
    Data = <<Time/binary, 16#01, VehicleState/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case03_reportinfo_0x02(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    DriverMotor1 = <<1, 1, 20, 2000:?WORD, 3000:?WORD, 80, 50000:?WORD, 12000:?WORD>>,
    DriverMotor2 = <<2, 2, 30, 3000:?WORD, 4000:?WORD, 90, 60000:?WORD, 13000:?WORD>>,
    Data = <<Time/binary, 16#02, 2, DriverMotor1/binary, DriverMotor2/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
                            <<"CtrlTemp">> := 20,
                            <<"Rotating">> := 2000,
                            <<"Torque">> := 3000,
                            <<"MotorTemp">> := 80,
                            <<"InputVoltage">> := 50000,
                            <<"DCBusCurrent">> := 12000
                        },
                        #{
                            <<"No">> := 2,
                            <<"Status">> := 2,
                            <<"CtrlTemp">> := 30,
                            <<"Rotating">> := 3000,
                            <<"Torque">> := 4000,
                            <<"MotorTemp">> := 90,
                            <<"InputVoltage">> := 60000,
                            <<"DCBusCurrent">> := 13000
                        }
                    ]
                }
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case04_reportinfo_0x03(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    FuelCell =
        <<12000:?WORD, 10000:?WORD, 40000:?WORD, 5:?WORD, 40, 41, 42, 43, 44, 1200:?WORD, 2,
            35000:?WORD, 3, 500:?WORD, 4, 1>>,
    Data = <<Time/binary, 16#03, FuelCell/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
                    <<"CellVoltage">> := 12000,
                    <<"CellCurrent">> := 10000,
                    <<"FuelConsumption">> := 40000,
                    <<"ProbeNum">> := 5,
                    <<"ProbeTemps">> := [40, 41, 42, 43, 44],
                    <<"H_MaxTemp">> := 1200,
                    <<"H_TempProbeCode">> := 2,
                    <<"H_MaxConc">> := 35000,
                    <<"H_ConcSensorCode">> := 3,
                    <<"H_MaxPress">> := 500,
                    <<"H_PressSensorCode">> := 4,
                    <<"DCStatus">> := 1
                }
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case05_reportinfo_0x04(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Info = <<1, 30000:?WORD, 40000:?WORD>>,
    Data = <<Time/binary, 16#04, Info/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
                    <<"Type">> := <<"Engine">>,
                    <<"Status">> := 1,
                    <<"CrankshaftSpeed">> := 30000,
                    <<"FuelConsumption">> := 40000
                }
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case06_reportinfo_0x05(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Info = <<6, 1234567:?DWORD, 7654321:?DWORD>>,
    Data = <<Time/binary, 16#05, Info/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
                    <<"Type">> := <<"Location">>,
                    <<"Status">> := 6,
                    <<"Longitude">> := 1234567,
                    <<"Latitude">> := 7654321
                }
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case07_reportinfo_0x06(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Info = <<12, 10, 7500:?WORD, 13, 11, 2000:?WORD, 14, 12, 120, 15, 13, 40>>,
    Data = <<Time/binary, 16#06, Info/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case08_reportinfo_0x07(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Info =
        <<2:?BYTE, 0:?DWORD, 1:?BYTE, 123:?DWORD, 2:?BYTE, 123:?DWORD, 223:?DWORD, 1:?BYTE,
            123:?DWORD, 1:?BYTE, 125:?DWORD>>,
    Data = <<Time/binary, 16#07, Info/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case09_reportinfo_0x08(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    VoltageSys1 = <<1:?BYTE, 5000:?WORD, 10000:?WORD, 2:?WORD, 0:?WORD, 1:?BYTE, 5000:?WORD>>,
    VoltageSys2 = <<2:?BYTE, 5001:?WORD, 10001:?WORD, 2:?WORD, 1:?WORD, 1:?BYTE, 5001:?WORD>>,
    Data = <<Time/binary, 16#08, 16#02, VoltageSys1/binary, VoltageSys2/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case10_reportinfo_0x09(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Temp1 = <<1:?BYTE, 10:?WORD, 5000:80>>,
    Temp2 = <<2:?BYTE, 1:?WORD, 100:?BYTE>>,
    Data = <<Time/binary, 16#09, 16#02, Temp1/binary, Temp2/binary>>,
    Packet = encode(?CMD_INFO_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/info">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case11_retx_report0x01(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    % REPORT RETX  data
    % - if auth success, not send ack, but will forward to emqx
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    VehicleState =
        <<1:?BYTE, 1:?BYTE, 1:?BYTE, 2000:?WORD, 999999:?DWORD, 5000:?WORD, 15000:?WORD, 50:?BYTE,
            1:?BYTE, 5:?BYTE, 6000:?WORD, 90:?BYTE, 0:?BYTE>>,
    Data = <<Time/binary, 16#01, VehicleState/binary>>,
    Packet = encode(?CMD_INFO_RE_REPORT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/reinfo">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_INFO_RE_REPORT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
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
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case12_vihecle_logout(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send VIHECLE_LOGOUT data
    %
    Time = <<16, 1, 1, 2, 59, 0>>,
    Info = <<1:?WORD>>,
    Data = <<Time/binary, Info/binary>>,
    Packet = encode(?CMD_VIHECLE_LOGOUT, <<"1G1BL52P7TR115520">>, Data),
    ok = gen_tcp:send(Socket, Packet),
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/vlogout">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_VIHECLE_LOGOUT,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 16,
                <<"Month">> := 1,
                <<"Day">> := 1,
                <<"Hour">> := 2,
                <<"Minute">> := 59,
                <<"Second">> := 0
            },
            <<"Seq">> := 1
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    ok.

t_case13_platform_login(_Config) ->
    % TODO: necessary?
    ok.

t_case14_platform_logout(_Config) ->
    % TODO: necessary?
    ok.

t_case15_heartbeat(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send HEARTBEAT
    %
    ReqBin = encode(?CMD_HEARTBEAT, <<"1G1BL52P7TR115520">>, <<>>),
    ok = gen_tcp:send(Socket, ReqBin),
    timer:sleep(200),

    %
    % recv server reply
    %
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    BodyLen = byte_size(Packet) - 3,
    <<"##", Body:BodyLen/binary, Crc>> = Packet,
    <<?CMD_HEARTBEAT, ?ACK_SUCCESS, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 6:?WORD, _:6/binary>> =
        Body,
    ?assertEqual(Crc, make_crc(Body, undefined)),
    ok.

t_case16_school_time(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send SCHOOL TIME
    %
    ReqBin = encode(?CMD_SCHOOL_TIME, <<"1G1BL52P7TR115520">>, <<>>),
    ok = gen_tcp:send(Socket, ReqBin),
    timer:sleep(200),

    %
    % recv server reply
    %
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    BodyLen = byte_size(Packet) - 3,
    <<"##", Body:BodyLen/binary, Crc>> = Packet,
    <<?CMD_SCHOOL_TIME, ?ACK_SUCCESS, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 6:?WORD, _:6/binary>> =
        Body,

    ?assertEqual(Crc, make_crc(Body, undefined)),
    ok.

t_case17_param_query(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send PARAM QUERY Request
    %
    Req = #{
        <<"Action">> => <<"Query">>,
        <<"Total">> => 2,
        <<"Ids">> => [<<"0x01">>, <<"0x02">>]
    },
    Topic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    BodyLen = byte_size(Packet) - 3,
    <<"##", Body:BodyLen/binary, Crc>> = Packet,
    <<?CMD_PARAM_QUERY, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 9:?WORD, _Time:6/binary, 2,
        1, 2>> = Body,

    ?assertEqual(Crc, make_crc(Body, undefined)),

    %
    % client reply
    %
    Time = <<17, 2, 2, 11, 12, 12>>,
    Info = <<2, 1, 6000:?WORD, 2, 10:?WORD>>,
    Resp = encode(
        ?CMD_PARAM_QUERY, ?ACK_SUCCESS, <<"1G1BL52P7TR115520">>, <<Time/binary, Info/binary>>
    ),
    gen_tcp:send(Socket, Resp),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_PARAM_QUERY,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Total">> := 2,
            <<"Params">> := [
                #{<<"0x01">> := 6000},
                #{<<"0x02">> := 10}
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),

    %
    % send PARAM QUERY - Query all of feild
    %
    Ids = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>,
    IdsMap = [
        <<"0x01">>,
        <<"0x02">>,
        <<"0x03">>,
        <<"0x04">>,
        <<"0x05">>,
        <<"0x06">>,
        <<"0x07">>,
        <<"0x08">>,
        <<"0x09">>,
        <<"0x0A">>,
        <<"0x0B">>,
        <<"0x0C">>,
        <<"0x0D">>,
        <<"0x0E">>,
        <<"0x0F">>,
        <<"0x10">>
    ],
    Req1 = #{
        <<"Action">> => <<"Query">>,
        <<"Total">> => 16,
        <<"Ids">> => IdsMap
    },
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet1} = gen_tcp:recv(Socket, 0, 500),
    BodyLen1 = byte_size(Packet1) - 3,
    <<"##", Body1:BodyLen1/binary, Crc1>> = Packet1,
    <<?CMD_PARAM_QUERY, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 23:?WORD, _:6/binary, 16,
        IdsRecved/binary>> = Body1,

    ?assertEqual(Ids, IdsRecved),
    ?assertEqual(Crc1, make_crc(Body1, undefined)),

    %
    % client reply
    %
    Time1 = <<17, 2, 2, 11, 12, 12>>,
    RespBin =
        <<16#01, 5000:?WORD, 16#02, 200:?WORD, 16#03, 30000:?WORD, 16#04, 10, 5, "google.com",
            16#06, 8080:?WORD, 16#07, "1.0.0", 16#08, "0.0.1", 16#09, 60:?BYTE, 16#0A, 60:?WORD,
            16#0B, 60:?WORD, 16#0C, 10:?BYTE, 16#0D, 14, 16#0E, "www.google.com", 16#0F, 9090:?WORD,
            16#10, 1:?BYTE>>,
    RespMap = [
        #{<<"0x01">> => 5000},
        #{<<"0x02">> => 200},
        #{<<"0x03">> => 30000},
        #{<<"0x04">> => 10},
        #{<<"0x05">> => <<"google.com">>},
        #{<<"0x06">> => 8080},
        #{<<"0x07">> => <<"1.0.0">>},
        #{<<"0x08">> => <<"0.0.1">>},
        #{<<"0x09">> => 60},
        #{<<"0x0A">> => 60},
        #{<<"0x0B">> => 60},
        #{<<"0x0C">> => 10},
        #{<<"0x0D">> => 14},
        #{<<"0x0E">> => <<"www.google.com">>},
        #{<<"0x0F">> => 9090},
        #{<<"0x10">> => 1}
    ],
    Resp1 = encode(
        ?CMD_PARAM_QUERY,
        ?ACK_SUCCESS,
        <<"1G1BL52P7TR115520">>,
        <<Time1/binary, 16, RespBin/binary>>
    ),
    gen_tcp:send(Socket, Resp1),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg1} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_PARAM_QUERY,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Total">> := 16,
            <<"Params">> := RespMapRecved
        }
    } = emqx_utils_json:decode(PubedMsg1, [return_maps]),
    ?assertEqual(RespMap, RespMapRecved),
    ok.

t_case18_param_setting(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send PARAM SETTING Request
    %
    Req = #{
        <<"Action">> => <<"Setting">>,
        <<"Total">> => 2,
        <<"Params">> => [
            #{<<"0x01">> => 5000},
            #{<<"0x02">> => 200}
        ]
    },
    Topic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    BodyLen = byte_size(Packet) - 3,
    <<"##", Body:BodyLen/binary, Crc>> = Packet,
    <<?CMD_PARAM_SETTING, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 13:?WORD, _Time:6/binary,
        2, 1, 5000:?WORD, 2, 200:?WORD>> = Body,

    ?assertEqual(Crc, make_crc(Body, undefined)),

    % FIXME: what's the conntent of the reply packet
    %        current is: generic reponse (update time field only)
    % client reply
    %
    Time = <<17, 2, 2, 11, 12, 12>>,
    Info = <<2, 1, 5000:?WORD, 2, 200:?WORD>>,
    Resp = encode(
        ?CMD_PARAM_SETTING, ?ACK_SUCCESS, <<"1G1BL52P7TR115520">>, <<Time/binary, Info/binary>>
    ),
    gen_tcp:send(Socket, Resp),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_PARAM_SETTING,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Total">> := 2,
            <<"Params">> := [
                #{<<"0x01">> := 5000},
                #{<<"0x02">> := 200}
            ]
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),
    %
    % send PARAM SETTING Request
    %
    ParamsBin =
        <<16#01, 5000:?WORD, 16#02, 200:?WORD, 16#03, 30000:?WORD, 16#04, 10, 5, "google.com",
            16#06, 8080:?WORD, 16#07, "1.0.0", 16#08, "0.0.1", 16#09, 60:?BYTE, 16#0A, 60:?WORD,
            16#0B, 60:?WORD, 16#0C, 10:?BYTE, 16#0D, 14, 16#0E, "www.google.com", 16#0F, 9090:?WORD,
            16#10, 1:?BYTE>>,
    ParamsMap = [
        #{<<"0x01">> => 5000},
        #{<<"0x02">> => 200},
        #{<<"0x03">> => 30000},
        #{<<"0x04">> => 10},
        #{<<"0x05">> => <<"google.com">>},
        #{<<"0x06">> => 8080},
        #{<<"0x07">> => <<"1.0.0">>},
        #{<<"0x08">> => <<"0.0.1">>},
        #{<<"0x09">> => 60},
        #{<<"0x0A">> => 60},
        #{<<"0x0B">> => 60},
        #{<<"0x0C">> => 10},
        #{<<"0x0D">> => 14},
        #{<<"0x0E">> => <<"www.google.com">>},
        #{<<"0x0F">> => 9090},
        #{<<"0x10">> => 1}
    ],
    Req1 = #{
        <<"Action">> => <<"Setting">>,
        <<"Total">> => 16,
        <<"Params">> => ParamsMap
    },
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet1} = gen_tcp:recv(Socket, 0, 500),
    BodyLen1 = byte_size(Packet1) - 3,
    <<"##", Body1:BodyLen1/binary, Crc1>> = Packet1,
    <<?CMD_PARAM_SETTING, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 76:?WORD, _:6/binary, 16,
        ParamsRecved/binary>> = Body1,

    ?assertEqual(ParamsBin, ParamsRecved),
    ?assertEqual(Crc1, make_crc(Body1, undefined)),

    % client reply
    %
    Time1 = <<17, 2, 2, 11, 12, 12>>,
    Resp1 = encode(
        ?CMD_PARAM_SETTING,
        ?ACK_SUCCESS,
        <<"1G1BL52P7TR115520">>,
        <<Time1/binary, 16, ParamsBin/binary>>
    ),
    gen_tcp:send(Socket, Resp1),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg1} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_PARAM_SETTING,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Total">> := 16,
            <<"Params">> := ParamsMapRecved
        }
    } = emqx_utils_json:decode(PubedMsg1, [return_maps]),
    ?assertEqual(ParamsMap, ParamsMapRecved),
    ok.

t_case19_terminal_ctrl(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send TERMINAL CTRL Request
    %
    Req = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => "0x02"
    },
    Topic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    BodyLen = byte_size(Packet) - 3,
    <<"##", Body:BodyLen/binary, Crc>> = Packet,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 7:?WORD, _:6/binary, 2>> =
        Body,

    ?assertEqual(Crc, make_crc(Body, undefined)),

    % FIXME: what's the conntent of the reply packet
    %        current is: generic reponse (update time field only)
    % client reply
    %
    Time = <<17, 2, 2, 11, 12, 12>>,
    Info = <<2>>,
    Resp = encode(
        ?CMD_TERMINAL_CTRL, ?ACK_SUCCESS, <<"1G1BL52P7TR115520">>, <<Time/binary, Info/binary>>
    ),
    gen_tcp:send(Socket, Resp),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_TERMINAL_CTRL,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Command">> := 2
        }
    } = emqx_utils_json:decode(PubedMsg, [return_maps]),

    %
    % send TERMINAL CTRL - REMOTE UPGRADE
    %
    UpgradeInfo =
        <<"emqtt;eusername;password;", 0, 0, 192, 168, 1, 1, ";", 8080:?WORD,
            ";BMW1;1.0.0;0.0.1;ftp://emqtt.io/ftp/server;", 10:?WORD>>,
    UpgradeMaps = #{
        <<"DialingName">> => <<"emqtt">>,
        <<"Username">> => <<"eusername">>,
        <<"Password">> => <<"password">>,
        <<"Ip">> => <<"192.168.1.1">>,
        <<"Port">> => 8080,
        <<"ManufacturerId">> => <<"BMW1">>,
        <<"HardwareVer">> => <<"1.0.0">>,
        <<"SoftwareVer">> => <<"0.0.1">>,
        <<"UpgradeUrl">> => <<"ftp://emqtt.io/ftp/server">>,
        <<"Timeout">> => 10
    },
    Req1 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x01">>,
        <<"Param">> => UpgradeMaps
    },
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet1} = gen_tcp:recv(Socket, 0, 500),
    BodyLen1 = byte_size(Packet1) - 3,
    <<"##", Body1:BodyLen1/binary, Crc1>> = Packet1,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 87:?WORD, _:6/binary, 1,
        UpgradeRecved/binary>> = Body1,

    ?assertEqual(UpgradeInfo, UpgradeRecved),
    ?assertEqual(Crc1, make_crc(Body1, undefined)),

    %
    % client reply
    %
    Time1 = <<17, 2, 2, 11, 12, 12>>,
    Resp1 = encode(
        ?CMD_TERMINAL_CTRL,
        ?ACK_SUCCESS,
        <<"1G1BL52P7TR115520">>,
        <<Time1/binary, 1, UpgradeInfo/binary>>
    ),
    gen_tcp:send(Socket, Resp1),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg1} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_TERMINAL_CTRL,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Command">> := 1,
            <<"Param">> := UpgradeMapsRecved
        }
    } = emqx_utils_json:decode(PubedMsg1, [return_maps]),

    ?assertEqual(UpgradeMaps, UpgradeMapsRecved),

    %
    % send TERMINAL CTRL - TERMINAL ALARM
    %
    AlarmInfo = <<1, "This is a configured alarm message!">>,
    AlarmMaps = #{
        <<"Level">> => 1,
        <<"Message">> => <<"This is a configured alarm message!">>
    },
    Req2 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x06">>,
        <<"Param">> => AlarmMaps
    },
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req2))),

    %
    % client get Command
    %
    timer:sleep(200),
    {ok, Packet2} = gen_tcp:recv(Socket, 0, 500),
    BodyLen2 = byte_size(Packet2) - 3,
    <<"##", Body2:BodyLen2/binary, Crc2>> = Packet2,
    <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 43:?WORD, _:6/binary, 6,
        AlarmRecved/binary>> = Body2,

    ?assertEqual(AlarmInfo, AlarmRecved),
    ?assertEqual(Crc2, make_crc(Body2, undefined)),

    %
    % client reply
    %
    Time2 = <<17, 2, 2, 11, 12, 12>>,
    Resp2 = encode(
        ?CMD_TERMINAL_CTRL,
        ?ACK_SUCCESS,
        <<"1G1BL52P7TR115520">>,
        <<Time2/binary, 6, AlarmInfo/binary>>
    ),
    gen_tcp:send(Socket, Resp2),

    %
    % emq get Response
    %
    timer:sleep(200),
    {<<"gbt32960/1G1BL52P7TR115520/upstream/response">>, PubedMsg2} = get_published_msg(),
    #{
        <<"Cmd">> := ?CMD_TERMINAL_CTRL,
        <<"Encrypt">> := ?ENCRYPT_NONE,
        <<"Vin">> := <<"1G1BL52P7TR115520">>,
        <<"Data">> := #{
            <<"Time">> := #{
                <<"Year">> := 17,
                <<"Month">> := 2,
                <<"Day">> := 2,
                <<"Hour">> := 11,
                <<"Minute">> := 12,
                <<"Second">> := 12
            },
            <<"Command">> := 6,
            <<"Param">> := AlarmMapsRecved
        }
    } = emqx_utils_json:decode(PubedMsg2, [return_maps]),

    ?assertEqual(AlarmMaps, AlarmMapsRecved),
    ok.

t_case20_proto_resend(_Config) ->
    emqx_logger:set_log_level(debug),

    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send TERMINAL CTRL Request
    %
    Req1 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x02">>
    },
    Req2 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x03">>
    },
    Req3 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x04">>
    },
    Req4 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x05">>
    },
    Topic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1))),
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req2))),
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req3))),
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req4))),

    %
    % client get Command
    %
    RecvAndCheck = fun(Sock, EexceptedPayload, WaitTime) ->
        {ok, Packet} = gen_tcp:recv(Sock, 0, 500),
        BodyLen = byte_size(Packet) - 3,
        <<"##", Body:BodyLen/binary, Crc>> = Packet,
        <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 7:?WORD, _:6/binary,
            RecvedPayload/binary>> = Body,
        ?assertEqual(EexceptedPayload, RecvedPayload),
        ?assertEqual(Crc, make_crc(Body, undefined)),
        timer:sleep(WaitTime)
    end,
    ReplyAck = fun(Sock, Payload) ->
        T = <<17, 2, 2, 11, 12, 12>>,
        F = encode(
            ?CMD_TERMINAL_CTRL, ?ACK_SUCCESS, <<"1G1BL52P7TR115520">>, <<T/binary, Payload/binary>>
        ),
        gen_tcp:send(Sock, F)
    end,
    %
    % server should re-send un-acked message
    %

    % regular send
    RecvAndCheck(Socket, <<2>>, 1000),
    % re-send turn 1
    RecvAndCheck(Socket, <<2>>, 1000),
    % re-send turn 2
    RecvAndCheck(Socket, <<2>>, 1000),
    % re-send turn 3; drop current message, send next
    RecvAndCheck(Socket, <<2>>, 1000),

    % check the next message
    RecvAndCheck(Socket, <<3>>, 1000),
    RecvAndCheck(Socket, <<3>>, 1000),
    RecvAndCheck(Socket, <<3>>, 500),
    ReplyAck(Socket, <<3>>),

    RecvAndCheck(Socket, <<4>>, 1000),
    RecvAndCheck(Socket, <<4>>, 500),
    ReplyAck(Socket, <<4>>),

    RecvAndCheck(Socket, <<5>>, 0),
    ReplyAck(Socket, <<5>>),

    %
    % Now, how check the inflight-window and mqueue is empty?
    %
    %     send & receive immediately!!
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1))),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),
    ok.

t_case21_proto_mqueue(_Config) ->
    % send VEHICLE LOGIN
    {ok, Socket} = login_first(),

    %
    % send TERMINAL CTRL Request
    %
    Req1 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x02">>
    },
    Req2 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x03">>
    },
    Req3 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x04">>
    },
    Req4 = #{
        <<"Action">> => <<"Control">>,
        <<"Command">> => <<"0x05">>
    },
    Topic = <<"gbt32960/1G1BL52P7TR115520/dnstream">>,
    [
        emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req1)))
     || _I <- lists:seq(1, 10)
    ],

    %% Wait the previous messages push to inflight
    timer:sleep(100),
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req2))),

    [
        emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req3)))
     || _I <- lists:seq(1, 10)
    ],

    %
    % client get Command
    %
    RecvAndCheck = fun(Sock, EexceptedPayload, WaitTime) ->
        {ok, Packet} = gen_tcp:recv(Sock, 0, 500),
        BodyLen = byte_size(Packet) - 3,
        <<"##", Body:BodyLen/binary, Crc>> = Packet,
        <<?CMD_TERMINAL_CTRL, ?ACK_IS_CMD, "1G1BL52P7TR115520", ?ENCRYPT_NONE, 7:?WORD, _:6/binary,
            RecvedPayload/binary>> = Body,
        ?assertEqual(EexceptedPayload, RecvedPayload),
        ?assertEqual(Crc, make_crc(Body, undefined)),
        timer:sleep(WaitTime)
    end,
    ReplyAck = fun(Sock, Payload) ->
        T = <<17, 2, 2, 11, 12, 12>>,
        F = encode(
            ?CMD_TERMINAL_CTRL, ?ACK_SUCCESS, <<"1G1BL52P7TR115520">>, <<T/binary, Payload/binary>>
        ),
        gen_tcp:send(Sock, F)
    end,

    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),
    RecvAndCheck(Socket, <<2>>, 0),
    ReplyAck(Socket, <<2>>),

    RecvAndCheck(Socket, <<3>>, 0),
    ReplyAck(Socket, <<3>>),

    %
    % Now, the message queue should be empty!
    %
    %     send & receive immediately!!
    emqx:publish(emqx_message:make(Topic, emqx_utils_json:encode(Req4))),
    RecvAndCheck(Socket, <<5>>, 0),
    ReplyAck(Socket, <<5>>),

    {error, timeout} = gen_tcp:recv(Socket, 0, 500),
    ok.
