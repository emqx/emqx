%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_jt808.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(FRM_FLAG, 16#7e:8).
-define(RESERVE, 0).
-define(NO_FRAGMENT, 0).
-define(WITH_FRAGMENT, 1).
-define(NO_ENCRYPT, 0).
-define(MSG_SIZE(X), X:10 / big - integer).

-define(WORD, 16 / big - integer).
-define(DWORD, 32 / big - integer).

-define(PORT, 6207).
-define(PORT_STR, "6207").
-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-define(PROTO_REG_SERVER_HOST, "http://127.0.0.1:8991").
-define(PROTO_REG_AUTH_PATH, "/jt808/auth").
-define(PROTO_REG_REGISTRY_PATH, "/jt808/registry").

-define(JT808_PHONE, "000123456789").
%% <<"jt808/000123456789/">>
-define(JT808_MOUNTPOINT, "jt808/" ?JT808_PHONE "/").
%% <<"jt808/000123456789/000123456789/up">>
-define(JT808_UP_TOPIC, <<?JT808_MOUNTPOINT, ?JT808_PHONE, "/up">>).
%% <<"jt808/000123456789/000123456789/dn">>
-define(JT808_DN_TOPIC, <<?JT808_MOUNTPOINT, ?JT808_PHONE, "/dn">>).

%% erlfmt-ignore
-define(CONF_DEFAULT, <<"
gateway.jt808 {
  listeners.tcp.default {
    bind = ", ?PORT_STR, "
  }
  proto {
    auth {
      allow_anonymous = false
      registry = \"", ?PROTO_REG_SERVER_HOST, ?PROTO_REG_REGISTRY_PATH, "\"
      authentication = \"", ?PROTO_REG_SERVER_HOST, ?PROTO_REG_AUTH_PATH, "\"
    }
  }
}
">>).

%% erlfmt-ignore
-define(CONF_ANONYMOUS, <<"
gateway.jt808 {
  listeners.tcp.default {
    bind = ", ?PORT_STR, "
  }
  proto {
    auth {
      allow_anonymous = true
    }
  }
}
">>).

%% erlfmt-ignore
-define(CONF_INVALID_AUTH_SERVER, <<"
gateway.jt808 {
  listeners.tcp.default {
    bind = ", ?PORT_STR, "
  }
  proto {
    auth {
      allow_anonymous = false
      registry = \"abc://abc\"
      authentication = \"abc://abc\"
    }
  }
}
">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case = t_case_invalid_auth_reg_server, Config) ->
    Apps = boot_apps(Case, ?CONF_INVALID_AUTH_SERVER, Config),
    [{suite_apps, Apps} | Config];
init_per_testcase(Case = t_case02_anonymous_register_and_auth, Config) ->
    Apps = boot_apps(Case, ?CONF_ANONYMOUS, Config),
    [{suite_apps, Apps} | Config];
init_per_testcase(Case, Config) when
    Case =:= t_create_ALLOW_invalid_auth_config;
    Case =:= t_create_DISALLOW_invalid_auth_config
->
    Apps = boot_apps(Case, <<>>, Config),
    [{suite_apps, Apps} | Config];
init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    Apps = boot_apps(Case, ?CONF_DEFAULT, Config),
    [{suite_apps, Apps} | Config].

end_per_testcase(_Case, Config) ->
    try
        ok = emqx_jt808_auth_http_test_server:stop()
    catch
        exit:noproc ->
            ok
    end,
    snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

boot_apps(Case, JT808Conf, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, JT808Conf},
            emqx_gateway
        ],
        #{work_dir => emqx_cth_suite:work_dir(Case, Config)}
    ),
    {ok, _Pid} = emqx_jt808_auth_http_test_server:start_link(),
    timer:sleep(1000),
    Apps.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% helper functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_packet(Header, Body) ->
    S1 = <<Header/binary, Body/binary>>,
    Crc = make_crc(S1, undefined),
    S2 = do_escape(<<S1/binary, Crc:8>>),
    Stream = <<16#7e:8, S2/binary, 16#7e:8>>,
    ?LOGT("encode a packet=~p", [binary_to_hex_string(Stream)]),
    Stream.

make_crc(<<>>, Xor) ->
    ?LOGT("crc is ~p", [Xor]),
    Xor;
make_crc(<<C:8, Rest/binary>>, undefined) ->
    make_crc(Rest, C);
make_crc(<<C:8, Rest/binary>>, Xor) ->
    make_crc(Rest, C bxor Xor).

do_escape(Binary) ->
    do_escape(Binary, <<>>).

do_escape(<<>>, Acc) ->
    Acc;
do_escape(<<16#7e, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, 16#7d, 16#02>>);
do_escape(<<16#7d, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, 16#7d, 16#01>>);
do_escape(<<C, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, C:8>>).

client_regi_procedure(Socket) ->
    client_regi_procedure(Socket, <<"123456">>).

client_regi_procedure(Socket, ExpectedAuthCode) ->
    %
    % send REGISTER
    %
    Manuf = <<"examp">>,
    Model = <<"33333333333333333", 0, 0, 0>>,
    DevId = <<"123456", 0>>,

    Color = 3,
    Plate = <<"ujvl239">>,
    RegisterPacket =
        <<58:?WORD, 59:?WORD, Manuf/binary, Model/binary, DevId/binary, Color, Plate/binary>>,
    MsgId = ?MC_REGISTER,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?WORD>>,
    S1 = gen_packet(Header, RegisterPacket),

    ok = gen_tcp:send(Socket, S1),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),

    AckPacket = <<MsgSn:?WORD, 0, ExpectedAuthCode/binary>>,
    Size2 = size(AckPacket),
    MsgId2 = ?MS_REGISTER_ACK,
    MsgSn2 = 0,
    Header2 =
        <<MsgId2:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size2),
            PhoneBCD/binary, MsgSn2:?WORD>>,
    S2 = gen_packet(Header2, AckPacket),
    ?LOGT("S2=~p", [binary_to_hex_string(S2)]),
    ?LOGT("Packet=~p", [binary_to_hex_string(Packet)]),
    ?assertEqual(S2, Packet),
    {ok, ExpectedAuthCode}.

client_auth_procedure(Socket, AuthCode) ->
    ?LOGT("start auth procedure", []),
    %
    % send AUTH
    %
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgId = ?MC_AUTH,
    MsgSn = 78,
    Size = size(AuthCode),
    Header =
        <<MsgId:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?WORD>>,
    S1 = gen_packet(Header, AuthCode),
    ?LOGT("auth S1=~p", [S1]),

    ok = gen_tcp:send(Socket, S1),
    %% timer:sleep(200),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket = <<MsgSn:?WORD, MsgId:?WORD, 0>>,
    Size2 = size(GenAckPacket),
    MsgId2 = ?MS_GENERAL_RESPONSE,
    MsgSn2 = 1,
    Header2 =
        <<MsgId2:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size2),
            PhoneBCD/binary, MsgSn2:?WORD>>,
    S2 = gen_packet(Header2, GenAckPacket),
    ?assertEqual(S2, Packet),
    ?assert(lists:member(?JT808_DN_TOPIC, emqx:topics())),

    ?LOGT("============= auth procedure success ===============", []).

client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD) ->
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MC_GENERAL_RESPONSE,
    MsgSn4 = 1,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    ok.

location_report() ->
    Alarm = 2,
    Status = 3,
    Latitude = 1000,
    Longitude = 1001,
    Altitude = 2000,
    Speed = 135,
    Direction = 32,
    TimeBCD = <<16#17, 16#10, 16#22, 16#11, 16#15, 16#53>>,
    Time = <<"171022111553">>,
    MileAge = 12379,
    FuelMeter = 972,
    Speed2 = 29,
    AlarmID = 18,
    OA_Type = 2,
    OA_Id = 29,
    IOA_Type = 8,
    IOA_Id = 23,
    IOA_Direction = 89,
    PTA_ID = 45,
    PTA_Time = 21,
    PTA_Result = 1,
    Binary =
        <<Alarm:?DWORD, Status:?DWORD, Latitude:?DWORD, Longitude:?DWORD, Altitude:?WORD,
            Speed:?WORD, Direction:?WORD, TimeBCD:6/binary, 16#01:8, 4:8, MileAge:?DWORD, 16#02:8,
            2:8, FuelMeter:?WORD, 16#03:8, 2:8, Speed2:?WORD, 16#04:8, 2:8, AlarmID:?WORD, 16#11:8,
            5:8, OA_Type:8, OA_Id:?DWORD, 16#12:8, 6:8, IOA_Type:8, IOA_Id:?DWORD, IOA_Direction:8,
            16#13:8, 7:8, PTA_ID:?DWORD, PTA_Time:?WORD, PTA_Result:8>>,
    Json = #{
        <<"alarm">> => Alarm,
        <<"status">> => Status,
        <<"latitude">> => Latitude,
        <<"longitude">> => Longitude,
        <<"altitude">> => Altitude,
        <<"speed">> => Speed,
        <<"direction">> => Direction,
        <<"time">> => Time,
        <<"extra">> => #{
            <<"mileage">> => MileAge,
            <<"fuel_meter">> => FuelMeter,
            <<"speed">> => Speed2,
            <<"alarm_id">> => AlarmID,
            <<"overspeed_alarm">> =>
                #{
                    <<"type">> => OA_Type,
                    <<"id">> => OA_Id
                },
            <<"in_out_alarm">> =>
                #{
                    <<"type">> => IOA_Type,
                    <<"id">> => IOA_Id,
                    <<"direction">> => IOA_Direction
                },
            <<"path_time_alarm">> =>
                #{
                    <<"id">> => PTA_ID,
                    <<"time">> => PTA_Time,
                    <<"result">> => PTA_Result
                }
        }
    },
    {Binary, Json}.

location_report_28bytes() ->
    Alarm = 2,
    Status = 3,
    Latitude = 1000,
    Longitude = 1001,
    Altitude = 2000,
    Speed = 135,
    Direction = 32,
    TimeBCD = <<16#17, 16#10, 16#22, 16#11, 16#15, 16#53>>,
    Time = <<"171022111553">>,
    Binary =
        <<Alarm:?DWORD, Status:?DWORD, Latitude:?DWORD, Longitude:?DWORD, Altitude:?WORD,
            Speed:?WORD, Direction:?WORD, TimeBCD:6/binary>>,
    Json = #{
        <<"alarm">> => Alarm,
        <<"status">> => Status,
        <<"latitude">> => Latitude,
        <<"longitude">> => Longitude,
        <<"altitude">> => Altitude,
        <<"speed">> => Speed,
        <<"direction">> => Direction,
        <<"time">> => Time
    },
    {Binary, Json}.

binary_to_hex_string(Data) ->
    lists:flatten([io_lib:format("~2.16.0B ", [X]) || <<X:8>> <= Data]).

receive_msg() ->
    receive
        {deliver, Topic, #message{payload = Payload}} ->
            {Topic, Payload}
    after 100 ->
        {error, timeout}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%% test cases %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

t_case00_register(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),

    {ok, AuthCode} = client_regi_procedure(Socket),
    ?assertEqual(AuthCode, <<"123456">>),

    ok = gen_tcp:close(Socket).

t_case01_auth(_) ->
    emqx_gateway_test_utils:meck_emqx_hook_calls(),

    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}, {nodelay, true}]),
    {ok, AuthCode} = client_regi_procedure(Socket),

    ok = client_auth_procedure(Socket, AuthCode),

    ?assertMatch(
        ['client.connect' | _],
        emqx_gateway_test_utils:collect_emqx_hooks_calls()
    ),

    ok = gen_tcp:close(Socket).

t_case02_anonymous_register_and_auth(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),

    {ok, AuthCode} = client_regi_procedure(Socket, <<>>),
    ?assertEqual(AuthCode, <<>>),

    ok = client_auth_procedure(Socket, AuthCode),

    ok = gen_tcp:close(Socket).

t_case03_heartbeat(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    %
    % send heartbeat
    %
    MsgId = ?MC_HEARTBEAT,
    MsgSn = 78,
    Size = 0,
    Header =
        <<MsgId:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?WORD>>,
    S1 = gen_packet(Header, <<>>),

    ok = gen_tcp:send(Socket, S1),
    %% timer:sleep(200),
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),

    GenAckPacket = <<MsgSn:?WORD, MsgId:?WORD, 0>>,
    Size2 = size(GenAckPacket),
    MsgId2 = ?MS_GENERAL_RESPONSE,
    MsgSn2 = 2,
    Header2 =
        <<MsgId2:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size2),
            PhoneBCD/binary, MsgSn2:?WORD>>,
    S2 = gen_packet(Header2, GenAckPacket),
    ?assertEqual(S2, Packet),

    ok = gen_tcp:close(Socket).

t_case04(_) ->
    ok = emqx:subscribe(?JT808_UP_TOPIC),

    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),

    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    %% send event report
    EventReportId = 98,
    MsgBody3 = <<EventReportId>>,
    MsgId3 = ?MC_EVENT_REPORT,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),
    timer:sleep(100),
    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{<<"id">> => EventReportId}
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case05(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),

    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    Flag = 15,
    Text = <<"who are you">>,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SEND_TEXT},
        <<"body">> => #{<<"flag">> => Flag, <<"text">> => Text}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),
    %
    % client get downlink "send text"
    %

    MsgBody3 = <<Flag:8, Text/binary>>,
    MsgId3 = ?MS_SEND_TEXT,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),
    ?LOGT("S3=~p", [S3]),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?assertEqual(S3, Packet3),

    % client send "client general response"
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MC_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    %% timer:sleep(300),

    ok = gen_tcp:close(Socket).

t_case06_downlink_retx(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),

    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    Flag = 15,
    Text = <<"who are you">>,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SEND_TEXT},
        <<"body">> => #{<<"flag">> => Flag, <<"text">> => Text}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),
    %
    % client get downlink "send text"
    %

    MsgBody3 = <<Flag:8, Text/binary>>,
    MsgId3 = ?MS_SEND_TEXT,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),
    ?LOGT("S3=~p", [S3]),

    %% wait emqx-jt808 to retx "send text"
    timer:sleep(100),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?assertEqual(S3, Packet3),

    % client send "client general response"
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MC_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    %% timer:sleep(300),

    % wait again, there should be no retx packet
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case07_dl_0x8302_send_question(_) ->
    ok = emqx:subscribe(?JT808_UP_TOPIC),

    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),

    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    Flag = 16#10,
    Question = <<"who are you">>,
    Length = size(Question),
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SEND_QUESTION},
        <<"body">> => #{
            <<"flag">> => Flag,
            <<"length">> => Length,
            <<"question">> => Question,
            <<"answers">> => [
                #{<<"id">> => 1, <<"len">> => 3, <<"answer">> => <<"Tom">>},
                #{<<"id">> => 2, <<"len">> => 4, <<"answer">> => <<"Mike">>}
            ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink "send text"
    MsgBody3 =
        <<Flag:8, Length:8, Question/binary, 1:8, 3:?WORD, <<"Tom">>/binary, 2:8, 4:?WORD,
            <<"Mike">>/binary>>,
    MsgId3 = ?MS_SEND_QUESTION,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?assertEqual(S3, Packet3),

    % client send "client answer"
    Answer = <<MsgSn3:?WORD, 2:8>>,
    Size4 = size(Answer),
    MsgId4 = ?MC_QUESTION_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, Answer),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId4,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size4,
                <<"msg_sn">> => MsgSn4
            },
            <<"body">> => #{<<"seq">> => MsgSn3, <<"id">> => 2}
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case08_dl_0x8500_vehicle_ctrl(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    Flag = 16#0,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_VEHICLE_CONTROL},
        <<"body">> => #{<<"flag">> => Flag}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink "vehicle ctrl"
    %
    MsgBody3 = <<Flag:8>>,
    MsgId3 = ?MS_VEHICLE_CONTROL,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?assertEqual(S3, Packet3),

    % client send "client answer"
    CtrlAck =
        <<126, 5, 0, 0, 30, 1, 136, 118, 99, 137, 114, 1, 244, 0, 7, 0, 0, 0, 0, 0, 4, 0, 0, 1, 49,
            122, 103, 6, 147, 104, 81, 0, 24, 0, 0, 0, 121, 23, 16, 32, 18, 3, 25, 69, 126>>,
    ?LOGT("S4 = ~p", [CtrlAck]),

    ok = gen_tcp:send(Socket, CtrlAck),
    timer:sleep(300),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> =>
                #{
                    <<"encrypt">> => 0,
                    <<"len">> => 30,
                    <<"msg_id">> => 1280,
                    <<"msg_sn">> => 500,
                    <<"phone">> => <<"018876638972">>
                },
            <<"body">> =>
                #{
                    <<"seq">> => 7,
                    <<"location">> => #{
                        <<"alarm">> => 0,
                        <<"altitude">> => 24,
                        <<"direction">> => 121,
                        <<"latitude">> => 20019815,
                        <<"longitude">> => 110323793,
                        <<"speed">> => 0,
                        <<"status">> => 262144,
                        <<"time">> => <<"171020120319">>
                    }
                }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case09_dl_0x8103_set_client_param(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    Length = 2,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_CLIENT_PARAM},
        <<"body">> => #{
            <<"length">> => Length,
            <<"params">> => [
                #{<<"id">> => 16#0013, <<"value">> => <<"www.example.com">>},
                #{<<"id">> => 16#0059, <<"value">> => 1200}
            ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink "vehicle ctrl"
    %
    MsgBody3 =
        <<Length:8, 16#0013:?DWORD, 15:8, <<"www.example.com">>/binary, 16#0059:?DWORD, 4:8,
            1200:?DWORD>>,
    MsgId3 = ?MS_SET_CLIENT_PARAM,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send "general response"
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MC_GENERAL_RESPONSE,
    MsgSn4 = 1,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),

    % no retrasmition of 0x8103
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case10_dl_0x8104_query_client_all_param(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_CLIENT_CONTROL},
        <<"body">> => #{<<"command">> => 200, <<"param">> => <<"ABCD">>}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<200:8, <<"ABCD">>/binary>>,
    MsgId3 = ?MS_CLIENT_CONTROL,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send ack
    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    %% timer:sleep(200),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case11_dl_0x8106_query_client_param(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_QUERY_CLIENT_PARAM},
        <<"body">> => #{<<"length">> => 2, <<"ids">> => [16#0092, 16#0031]}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, 16#0092:?DWORD, 16#0031:?DWORD>>,
    MsgId3 = ?MS_QUERY_CLIENT_PARAM,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    UlPacket4 = <<MsgSn3:?WORD, 2:8, 16#0031:?DWORD, 2:8, 379:?WORD, 16#0092:?DWORD, 1:8, 2:8>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_QUERY_PARAM_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_QUERY_PARAM_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"length">> => 2,
                <<"params">> => [
                    #{<<"id">> => 16#0031, <<"value">> => 379},
                    #{<<"id">> => 16#0092, <<"value">> => 2}
                ]
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    %% timer:sleep(200),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case11_dl_0x8107_query_client_attrib(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{<<"header">> => #{<<"msg_id">> => ?MS_QUERY_CLIENT_ATTRIB}},
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<>>,
    MsgId3 = ?MS_QUERY_CLIENT_ATTRIB,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    timer:sleep(100),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    UlPacket4 =
        <<12:?WORD, <<"manu3">>/binary, <<"A1B2C3D4E5F6G7H8I", 0, 0, 0>>:20/binary,
            <<"dev123", 0>>:7/binary,
            <<16#33, 16#33, 16#33, 16#33, 16#33, 16#44, 16#44, 16#44, 16#44, 16#44>>:10/binary, 6:8,
            <<"v2.3.7">>:6/binary, 5:8, <<"v1.26">>:5/binary, 101:8, 102:8>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_QUERY_ATTRIB_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_QUERY_ATTRIB_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"type">> => 12,
                <<"manufacturer">> => <<"manu3">>,
                <<"model">> => <<"A1B2C3D4E5F6G7H8I">>,
                <<"id">> => <<"dev123">>,
                <<"iccid">> => <<"33333333334444444444">>,
                <<"hardware_version">> => <<"v2.3.7">>,
                <<"firmware_version">> => <<"v1.26">>,
                <<"gnss_prop">> => 101,
                <<"comm_prop">> => 102
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case15_dl_0x8201_query_location(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{<<"header">> => #{<<"msg_id">> => ?MS_QUERY_LOCATION}},
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<>>,
    MsgId3 = ?MS_QUERY_LOCATION,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    {LocationReportBinary, LocationReportJson} = location_report(),
    UlPacket4 = <<MsgSn3:?WORD, LocationReportBinary/binary>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_QUERY_LOCATION_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_QUERY_LOCATION_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"params">> => LocationReportJson
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    %% timer:sleep(200),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_location_report(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    {LocationReportBinary, LocationReportJson} = location_report(),
    UlPacket = <<LocationReportBinary/binary>>,
    Size = size(UlPacket),
    MsgId = ?MC_LOCATION_REPORT,
    MsgSn = 1,
    Header =
        <<MsgId:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?WORD>>,
    S = gen_packet(Header, UlPacket),
    ?LOGT("S = ~p", [S]),

    ok = gen_tcp:send(Socket, S),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size,
                <<"msg_id">> => ?MC_LOCATION_REPORT,
                <<"msg_sn">> => MsgSn,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => LocationReportJson
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % receive general response
    {ok, Packet} = gen_tcp:recv(Socket, 0, 500),
    GenAckPacket = <<MsgSn:?WORD, MsgId:?WORD, 0>>,
    Size2 = size(GenAckPacket),
    MsgId2 = ?MS_GENERAL_RESPONSE,
    MsgSn2 = 2,
    Header2 =
        <<MsgId2:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size2),
            PhoneBCD/binary, MsgSn2:?WORD>>,
    S2 = gen_packet(Header2, GenAckPacket),
    ?assertEqual(S2, Packet),

    % no retrasmition of downlink message
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case15_dl_0x8202_trace_location(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_TRACE_LOCATION},
        <<"body">> => #{<<"period">> => 23, <<"expiry">> => 183}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<23:?WORD, 183:?DWORD>>,
    MsgId3 = ?MS_TRACE_LOCATION,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case50_ul_0x0303_info_request_cancel(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 = <<1:8, 6:8>>,
    MsgId3 = ?MC_INFO_REQ_CANCEL,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %%timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{<<"id">> => 1, <<"flag">> => 6}
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case51_ul_0x0701_waybill_report(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 = <<7:?DWORD, <<"ABCDEFG">>/binary>>,
    MsgId3 = ?MC_WAYBILL_REPORT,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %% timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{<<"length">> => 7, <<"data">> => base64:encode(<<"ABCDEFG">>)}
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case52_ul_0x0705_can_bus_report(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 =
        <<2:?WORD, <<16#09, 16#23, 16#46, 16#07, 16#25>>/binary, 0:1, 1:1, 0:1, 35:29,
            <<"11111111">>/binary, 1:1, 0:1, 1:1, 36:29, <<"22222222">>/binary>>,
    MsgId3 = ?MC_CAN_BUS_REPORT,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %% timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{
                <<"length">> => 2,
                <<"time">> => <<"0923460725">>,
                <<"can_data">> => [
                    #{
                        <<"channel">> => 0,
                        <<"frame_type">> => 1,
                        <<"data_method">> => 0,
                        <<"id">> => 35,
                        <<"data">> => base64:encode(<<"11111111">>)
                    },
                    #{
                        <<"channel">> => 1,
                        <<"frame_type">> => 0,
                        <<"data_method">> => 1,
                        <<"id">> => 36,
                        <<"data">> => base64:encode(<<"22222222">>)
                    }
                ]
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case53_ul_0x0800_multimedia_event_report(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 = <<65:?DWORD, 2:8, 1:8, 4:8, 103:8>>,
    MsgId3 = ?MC_MULTIMEDIA_EVENT_REPORT,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %% timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{
                <<"id">> => 65,
                <<"type">> => 2,
                <<"format">> => 1,
                <<"event">> => 4,
                <<"channel">> => 103
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case54_ul_0x0900_send_transparent_data(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 = <<39:8, <<"oufwei">>/binary>>,
    MsgId3 = ?MC_SEND_TRANSPARENT_DATA,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %% timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % Receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{<<"type">> => 39, <<"data">> => base64:encode(<<"oufwei">>)}
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    ok = gen_tcp:close(Socket).

t_case55_ul_0x0901_send_zip_data(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    %
    % send event report
    %
    MsgBody3 = <<4:?DWORD, <<"1234">>/binary>>,
    MsgId3 = ?MC_SEND_ZIP_DATA,
    MsgSn3 = 79,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    ok = gen_tcp:send(Socket, S3),
    %% timer:sleep(600),
    {ok, Packet4} = gen_tcp:recv(Socket, 0, 500),

    % receive general response
    GenAckPacket4 = <<MsgSn3:?WORD, MsgId3:?WORD, 0>>,
    Size4 = size(GenAckPacket4),
    MsgId4 = ?MS_GENERAL_RESPONSE,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, GenAckPacket4),
    ?assertEqual(S4, Packet4),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId3,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size3,
                <<"msg_sn">> => MsgSn3
            },
            <<"body">> => #{
                <<"length">> => 4,
                <<"data">> => base64:encode(<<"1234">>)
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    ok = gen_tcp:close(Socket).

t_case16_dl_0x8301_set_event(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_EVENT},
        <<"body">> => #{
            <<"type">> => 3,
            <<"length">> => 2,
            <<"events">> =>
                [
                    #{<<"id">> => 56, <<"length">> => 3, <<"content">> => <<"111">>},
                    #{<<"id">> => 7, <<"length">> => 5, <<"content">> => <<"nwKdmww">>}
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<3:8, 2:8, 56:8, 3:8, <<"111">>/binary, 7:8, 5:8, <<"nwKdmww">>/binary>>,
    MsgId3 = ?MS_SET_EVENT,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case17_dl_0x8303_set_menu(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_MENU},
        <<"body">> => #{
            <<"type">> => 3,
            <<"length">> => 2,
            <<"menus">> =>
                [
                    #{<<"type">> => 56, <<"length">> => 3, <<"info">> => <<"111">>},
                    #{<<"type">> => 7, <<"length">> => 5, <<"info">> => <<"nwKdmww">>}
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink command
    MsgBody3 = <<3:8, 2:8, 56:8, 3:?WORD, <<"111">>/binary, 7:8, 5:?WORD, <<"nwKdmww">>/binary>>,
    MsgId3 = ?MS_SET_MENU,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case18_dl_0x8304_info_content(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_INFO_CONTENT},
        <<"body">> => #{<<"type">> => 3, <<"length">> => 2, <<"info">> => <<"NY">>}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink command
    MsgBody3 = <<3:8, 2:?WORD, <<"NY">>/binary>>,
    MsgId3 = ?MS_INFO_CONTENT,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case19_dl_0x8400_phone_callback(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_PHONE_CALLBACK},
        <<"body">> => #{<<"type">> => 0, <<"phone">> => <<"15632597856">>}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink command
    MsgBody3 = <<0:8, <<"15632597856">>/binary>>,
    MsgId3 = ?MS_PHONE_CALLBACK,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case20_dl_0x8401_set_phone_number(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_PHONE_NUMBER},
        <<"body">> => #{
            <<"type">> => 2,
            <<"length">> => 2,
            <<"contacts">> =>
                [
                    #{
                        <<"type">> => 2,
                        <<"phone_len">> => 10,
                        <<"phone">> => <<"13011112222">>,
                        <<"name_len">> => 3,
                        <<"name">> => <<"Tom">>
                    },
                    #{
                        <<"type">> => 3,
                        <<"phone_len">> => 11,
                        <<"phone">> => <<"013011113333">>,
                        <<"name_len">> => 4,
                        <<"name">> => <<"Mike">>
                    }
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<2:8, 2:8, 2:8, 10:8, <<"13011112222">>/binary, 3:8, <<"Tom">>/binary, 3:8, 11:8,
            <<"013011113333">>/binary, 4:8, <<"Mike">>/binary>>,
    MsgId3 = ?MS_SET_PHONE_NUMBER,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case21_dl_0x8600_set_circle_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_CIRCLE_AREA},
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 2,
            <<"areas">> =>
                [
                    #{
                        <<"id">> => 267,
                        <<"flag">> => 36,
                        <<"center_latitude">> => 20057,
                        <<"center_longitude">> => 4529,
                        <<"radius">> => 279,
                        <<"start_time">> => <<"170912103253">>,
                        <<"end_time">> => <<"170913103253">>,
                        <<"max_speed">> => 120,
                        <<"overspeed_duration">> => 36
                    },
                    #{
                        <<"id">> => 355,
                        <<"flag">> => 36,
                        <<"center_latitude">> => 20057,
                        <<"center_longitude">> => 4529,
                        <<"radius">> => 132,
                        <<"start_time">> => <<"170912103253">>,
                        <<"end_time">> => <<"170913103253">>,
                        <<"max_speed">> => 120,
                        <<"overspeed_duration">> => 36
                    }
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink command
    MsgBody3 =
        <<0:8, 2:8, 267:?DWORD, 36:?WORD, 20057:?DWORD, 4529:?DWORD, 279:?DWORD,
            <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 120:?WORD, 36:8, 355:?DWORD,
            36:?WORD, 20057:?DWORD, 4529:?DWORD, 132:?DWORD,
            <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 120:?WORD, 36:8>>,
    MsgId3 = ?MS_SET_CIRCLE_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case22_dl_0x8601_del_circle_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DEL_CIRCLE_AREA},
        <<"body">> => #{<<"length">> => 2, <<"ids">> => [3, 78]}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, 3:?DWORD, 78:?DWORD>>,
    MsgId3 = ?MS_DEL_CIRCLE_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case23_dl_0x8602_set_rect_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_RECT_AREA},
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 2,
            <<"areas">> =>
                [
                    #{
                        <<"id">> => 267,
                        <<"flag">> => 36,
                        <<"lt_lat">> => 20057,
                        <<"lt_lng">> => 4529,
                        <<"rb_lat">> => 30057,
                        <<"rb_lng">> => 5529,
                        <<"start_time">> => <<"170912103253">>,
                        <<"end_time">> => <<"170913103253">>,
                        <<"max_speed">> => 120,
                        <<"overspeed_duration">> => 36
                    },
                    #{
                        <<"id">> => 355,
                        <<"flag">> => 36,
                        <<"lt_lat">> => 20057,
                        <<"lt_lng">> => 4529,
                        <<"rb_lat">> => 30057,
                        <<"rb_lng">> => 5529,
                        <<"start_time">> => <<"170912103253">>,
                        <<"end_time">> => <<"170913103253">>,
                        <<"max_speed">> => 120,
                        <<"overspeed_duration">> => 36
                    }
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<0:8, 2:8, 267:?DWORD, 36:?WORD, 20057:?DWORD, 4529:?DWORD, 30057:?DWORD, 5529:?DWORD,
            <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 120:?WORD, 36:8, 355:?DWORD,
            36:?WORD, 20057:?DWORD, 4529:?DWORD, 30057:?DWORD, 5529:?DWORD,
            <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 120:?WORD, 36:8>>,
    MsgId3 = ?MS_SET_RECT_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case24_dl_0x8603_del_circle_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DEL_RECT_AREA},
        <<"body">> => #{<<"length">> => 2, <<"ids">> => [3, 78]}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, 3:?DWORD, 78:?DWORD>>,
    MsgId3 = ?MS_DEL_RECT_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case25_dl_0x8604_set_poly_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_POLY_AREA},
        <<"body">> => #{
            <<"id">> => 267,
            <<"flag">> => 36,
            <<"start_time">> => <<"170912103253">>,
            <<"end_time">> => <<"170913103253">>,
            <<"max_speed">> => 120,
            <<"overspeed_duration">> => 36,
            <<"length">> => 3,
            <<"points">> =>
                [
                    #{<<"lat">> => 20057, <<"lng">> => 4529},
                    #{<<"lat">> => 21057, <<"lng">> => 14569},
                    #{<<"lat">> => 7032, <<"lng">> => 429}
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<267:?DWORD, 36:?WORD, <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 120:?WORD, 36:8, 3:?WORD,
            20057:?DWORD, 4529:?DWORD, 21057:?DWORD, 14569:?DWORD, 7032:?DWORD, 429:?DWORD>>,
    MsgId3 = ?MS_SET_POLY_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case26_dl_0x8605_del_poly_area(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DEL_POLY_AREA},
        <<"body">> => #{<<"length">> => 2, <<"ids">> => [3, 78]}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, 3:?DWORD, 78:?DWORD>>,
    MsgId3 = ?MS_DEL_POLY_AREA,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case27_dl_0x8606_set_path(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SET_PATH},
        <<"body">> => #{
            <<"id">> => 267,
            <<"flag">> => 36,
            <<"start_time">> => <<"170912103253">>,
            <<"end_time">> => <<"170913103253">>,
            <<"length">> => 2,
            <<"points">> =>
                [
                    #{
                        <<"point_id">> => 3,
                        <<"path_id">> => 71,
                        <<"point_lat">> => 7324,
                        <<"point_lng">> => 9732,
                        <<"width">> => 54,
                        <<"attrib">> => 23,
                        <<"passed">> => 0,
                        <<"uncovered">> => 1,
                        <<"max_speed">> => 132,
                        <<"overspeed_duration">> => 4
                    },
                    #{
                        <<"point_id">> => 4,
                        <<"path_id">> => 72,
                        <<"point_lat">> => 7324,
                        <<"point_lng">> => 9732,
                        <<"width">> => 54,
                        <<"attrib">> => 23,
                        <<"passed">> => 0,
                        <<"uncovered">> => 1,
                        <<"max_speed">> => 169,
                        <<"overspeed_duration">> => 69
                    }
                ]
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<267:?DWORD, 36:?WORD, <<16#17, 16#09, 16#12, 16#10, 16#32, 16#53>>/binary,
            <<16#17, 16#09, 16#13, 16#10, 16#32, 16#53>>/binary, 2:?WORD, 3:?DWORD, 71:?DWORD,
            7324:?DWORD, 9732:?DWORD, 54:8, 23:8, 0:?WORD, 1:?WORD, 132:?WORD, 4:8, 4:?DWORD,
            72:?DWORD, 7324:?DWORD, 9732:?DWORD, 54:8, 23:8, 0:?WORD, 1:?WORD, 169:?WORD, 69:8>>,
    MsgId3 = ?MS_SET_PATH,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case26_dl_0x8607_del_path(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DEL_PATH},
        <<"body">> => #{<<"length">> => 2, <<"ids">> => [3, 78]}
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, 3:?DWORD, 78:?DWORD>>,
    MsgId3 = ?MS_DEL_PATH,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case27_dl_0x8700_drive_record_capture(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    CaptureCmd = 2,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DRIVE_RECORD_CAPTURE},
        <<"body">> => #{
            <<"command">> => CaptureCmd, <<"param">> => base64:encode(<<"000123456789">>)
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<2:8, <<"000123456789">>/binary>>,
    MsgId3 = ?MS_DRIVE_RECORD_CAPTURE,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send "drive record report"
    UlPacket4 = <<MsgSn3:?WORD, CaptureCmd:8, <<"77777">>/binary>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_DRIVE_RECORD_REPORT,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_DRIVE_RECORD_REPORT,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"command">> => CaptureCmd,
                <<"data">> => base64:encode(<<"77777">>)
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % no retrasmition of downlink message
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case28_dl_0x8701_drive_record_param_send(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    CaptureCmd = 2,
    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_DRIVE_REC_PARAM_SEND},
        <<"body">> => #{
            <<"command">> => CaptureCmd, <<"param">> => base64:encode(<<"000123456789">>)
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<CaptureCmd:8, <<"000123456789">>/binary>>,
    MsgId3 = ?MS_DRIVE_REC_PARAM_SEND,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case29_dl_0x8702_request_driver_id(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{<<"header">> => #{<<"msg_id">> => ?MS_REQ_DRIVER_ID}},
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<>>,
    MsgId3 = ?MS_REQ_DRIVER_ID,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send "drive record report"
    UlPacket4 =
        <<1:8, 16#17, 16#10, 16#12, 16#09, 16#03, 16#52, 0:8, 3:8, <<"Tom">>/binary,
            <<"77778888999900001111">>/binary, 6:8, <<"org123">>/binary, 16#20, 16#30, 16#12,
            16#31>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_DRIVER_ID_REPORT,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_DRIVER_ID_REPORT,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"status">> => 1,
                <<"time">> => <<"171012090352">>,
                <<"ic_result">> => 0,
                <<"driver_name">> => <<"Tom">>,
                <<"certificate">> => <<"77778888999900001111">>,
                <<"organization">> => <<"org123">>,
                <<"cert_expiry">> => <<"20301231">>
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case30_dl_0x8801_camera_shot(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_CAMERA_SHOT},
        <<"body">> => #{
            <<"channel_id">> => 172,
            <<"command">> => 5,
            <<"period">> => 2,
            <<"save">> => 1,
            <<"resolution">> => 8,
            <<"quality">> => 3,
            <<"bright">> => 4,
            <<"contrast">> => 5,
            <<"saturate">> => 6,
            <<"chromaticity">> => 7
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<172:8, 5:?WORD, 2:?WORD, 1:8, 8:8, 3:8, 4:8, 5:8, 6:8, 7:8>>,
    MsgId3 = ?MS_CAMERA_SHOT,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send "camera shot ack"
    UlPacket4 = <<MsgSn3:?WORD, 0:8, 2:?WORD, 220:?DWORD, 221:?DWORD>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_CAMERA_SHOT_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_CAMERA_SHOT_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"result">> => 0,
                <<"length">> => 2,
                <<"ids">> => [220, 221]
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % No retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case31_dl_0x8802_mm_data_search(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_MM_DATA_SEARCH},
        <<"body">> => #{
            <<"type">> => 0,
            <<"channel">> => 17,
            <<"event">> => 2,
            <<"start_time">> => <<"170923144607">>,
            <<"end_time">> => <<"170923145826">>
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<0:8, 17:8, 2:8, 16#17, 16#09, 16#23, 16#14, 16#46, 16#07, 16#17, 16#09, 16#23, 16#14,
            16#58, 16#26>>,
    MsgId3 = ?MS_MM_DATA_SEARCH,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    % client send "mm data search ack"
    {LocBin, LocJson} = location_report_28bytes(),
    UlPacket4 =
        <<MsgSn3:?WORD, 2:?WORD, 25:?DWORD, 1:8, 97:8, 1:8, LocBin/binary, 26:?DWORD, 2:8, 98:8,
            3:8, LocBin/binary>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_MM_DATA_SEARCH_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_MM_DATA_SEARCH_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"length">> => 2,
                <<"result">> => [
                    #{
                        <<"id">> => 25,
                        <<"type">> => 1,
                        <<"channel">> => 97,
                        <<"event">> => 1,
                        <<"location">> => LocJson
                    },
                    #{
                        <<"id">> => 26,
                        <<"type">> => 2,
                        <<"channel">> => 98,
                        <<"event">> => 3,
                        <<"location">> => LocJson
                    }
                ]
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    %% No retrasmition of downlink message
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case32_dl_0x8803_mm_data_upload(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_MM_DATA_UPLOAD},
        <<"body">> => #{
            <<"type">> => 0,
            <<"channel">> => 17,
            <<"event">> => 2,
            <<"start_time">> => <<"170923144607">>,
            <<"end_time">> => <<"170923145826">>,
            <<"delete">> => 1
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 =
        <<0:8, 17:8, 2:8, 16#17, 16#09, 16#23, 16#14, 16#46, 16#07, 16#17, 16#09, 16#23, 16#14,
            16#58, 16#26, 1:8>>,
    MsgId3 = ?MS_MM_DATA_UPLOAD,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case33_dl_0x8804_voice_record(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_VOICE_RECORD},
        <<"body">> => #{
            <<"command">> => 1,
            <<"time">> => 2,
            <<"save">> => 3,
            <<"rate">> => 4
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %
    % client get downlink command
    %
    MsgBody3 = <<1:8, 2:?WORD, 3:8, 4:8>>,
    MsgId3 = ?MS_VOICE_RECORD,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    client_send_general_response(Socket, MsgId3, MsgSn3, PhoneBCD),

    % no retrasmition of downlink message
    %% timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case34_dl_0x8805_single_mm_data_ctrl(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    ok = emqx:subscribe(?JT808_UP_TOPIC),

    DlCommand = #{
        <<"header">> => #{<<"msg_id">> => ?MS_SINGLE_MM_DATA_CTRL},
        <<"body">> => #{
            <<"id">> => 30,
            <<"flag">> => 40
        }
    },
    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),

    %% client get downlink command
    MsgBody3 = <<30:?DWORD, 40:8>>,
    MsgId3 = ?MS_SINGLE_MM_DATA_CTRL,
    MsgSn3 = 2,
    Size3 = size(MsgBody3),
    Header3 =
        <<MsgId3:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size3),
            PhoneBCD/binary, MsgSn3:?WORD>>,
    S3 = gen_packet(Header3, MsgBody3),

    %% timer:sleep(600),
    {ok, Packet3} = gen_tcp:recv(Socket, 0, 500),
    ?LOGT("     S3=~p", [binary_to_hex_string(S3)]),
    ?LOGT("Packet3=~p", [binary_to_hex_string(Packet3)]),
    ?assertEqual(S3, Packet3),

    ?LOGT("client receive command from server ~p", [S3]),

    {LocBin, LocJson} = location_report_28bytes(),
    UlPacket4 =
        <<MsgSn3:?WORD, 2:?WORD, 25:?DWORD, 1:8, 97:8, 1:8, LocBin/binary, 26:?DWORD, 2:8, 98:8,
            3:8, LocBin/binary>>,
    Size4 = size(UlPacket4),
    MsgId4 = ?MC_MM_DATA_SEARCH_ACK,
    MsgSn4 = 2,
    Header4 =
        <<MsgId4:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size4),
            PhoneBCD/binary, MsgSn4:?WORD>>,
    S4 = gen_packet(Header4, UlPacket4),
    ?LOGT("S4 = ~p", [S4]),

    ok = gen_tcp:send(Socket, S4),
    timer:sleep(100),

    {?JT808_UP_TOPIC, Payload} = receive_msg(),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"encrypt">> => 0,
                <<"len">> => Size4,
                <<"msg_id">> => ?MC_MM_DATA_SEARCH_ACK,
                <<"msg_sn">> => 2,
                <<"phone">> => <<"000123456789">>
            },
            <<"body">> => #{
                <<"seq">> => MsgSn3,
                <<"length">> => 2,
                <<"result">> => [
                    #{
                        <<"id">> => 25,
                        <<"type">> => 1,
                        <<"channel">> => 97,
                        <<"event">> => 1,
                        <<"location">> => LocJson
                    },
                    #{
                        <<"id">> => 26,
                        <<"type">> => 2,
                        <<"channel">> => 98,
                        <<"event">> => 3,
                        <<"location">> => LocJson
                    }
                ]
            }
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),

    % no retrasmition of downlink message
    %%timer:sleep(10000),
    {error, timeout} = gen_tcp:recv(Socket, 0, 500),

    ok = gen_tcp:close(Socket).

t_case_dl_invalid_msg(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    {ok, AuthCode} = client_regi_procedure(Socket),
    ok = client_auth_procedure(Socket, AuthCode),
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    DlCommand = #{
        %% missing msg_id
        <<"header">> => #{},
        <<"body">> => #{
            <<"id">> => 30,
            <<"flag">> => 40
        }
    },

    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, emqx_utils_json:encode(DlCommand))),
    ?block_until(#{?snk_kind := invalid_dl_message}),

    emqx:publish(emqx_message:make(?JT808_DN_TOPIC, <<"invliad_json_str">>)),
    ?block_until(#{?snk_kind := invalid_dl_message}),

    ok = gen_tcp:close(Socket).

t_case_invalid_auth_reg_server(_Config) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, ?PORT, [binary, {active, false}]),
    %
    % send REGISTER
    %
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,

    Color = 3,
    Plate = <<"ujvl239">>,
    RegisterPacket =
        <<58:?WORD, 59:?WORD, Manuf/binary, Model/binary, DevId/binary, Color, Plate/binary>>,
    MsgId = ?MC_REGISTER,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?WORD>>,
    S1 = gen_packet(Header, RegisterPacket),

    %% Send REGISTER Packet
    ok = gen_tcp:send(Socket, S1),
    %% Receive REGISTER_ACK Packet
    {ok, RecvPacket} = gen_tcp:recv(Socket, 0, 50_000),

    %% No AuthCode when register failed
    AuthCode = <<>>,

    AckPacket = <<MsgSn:?WORD, 1, AuthCode/binary>>,
    Size2 = size(AckPacket),
    MsgId2 = ?MS_REGISTER_ACK,
    MsgSn2 = 0,
    Header2 =
        <<MsgId2:?WORD, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size2),
            PhoneBCD/binary, MsgSn2:?WORD>>,
    S2 = gen_packet(Header2, AckPacket),

    ?LOGT("S1=~p", [binary_to_hex_string(S1)]),
    ?LOGT("S2=~p", [binary_to_hex_string(S2)]),
    ?LOGT("Received REGISTER_ACK Packet=~p", [binary_to_hex_string(RecvPacket)]),

    ?assertEqual(S2, RecvPacket),
    ok.

t_create_ALLOW_invalid_auth_config(_Config) ->
    test_invalid_config(create, true).

t_create_DISALLOW_invalid_auth_config(_Config) ->
    test_invalid_config(create, false).

t_update_ALLOW_invalid_auth_config(_Config) ->
    test_invalid_config(update, true).

t_update_DISALLOW_invalid_auth_config(_Config) ->
    test_invalid_config(update, false).

test_invalid_config(CreateOrUpdate, AnonymousAllowed) ->
    InvalidConfig = raw_jt808_config(AnonymousAllowed),
    UpdateResult = create_or_update(CreateOrUpdate, InvalidConfig),
    ?assertMatch(
        {error, #{
            kind := validation_error,
            reason := matched_no_union_member,
            path := "gateway.jt808.proto.auth"
        }},
        UpdateResult
    ).

create_or_update(create, InvalidConfig) ->
    emqx_gateway_conf:load_gateway(jt808, InvalidConfig);
create_or_update(update, InvalidConfig) ->
    emqx_gateway_conf:update_gateway(jt808, InvalidConfig).

%% Allow: allow anonymous connection, registry and authentication URL not required.
raw_jt808_config(Allow = true) ->
    AuthConfig = #{
        <<"auth">> => #{
            <<"allow_anonymous">> => Allow,
            %% registry and authentication `NOT REQUIRED`, but can be configured
            <<"registry">> => <<?PROTO_REG_SERVER_HOST, ?PROTO_REG_REGISTRY_PATH>>,
            <<"authentication">> => <<?PROTO_REG_SERVER_HOST, ?PROTO_REG_AUTH_PATH>>,
            <<"BADKEY_registry_url">> => <<?PROTO_REG_SERVER_HOST, ?PROTO_REG_REGISTRY_PATH>>
        }
    },
    emqx_utils_maps:deep_merge(raw_jt808_config(), #{<<"proto">> => AuthConfig});
%% DisAllow: required registry and authentication URL configuration to auth client.
raw_jt808_config(DisAllow = false) ->
    AuthConfig = #{
        <<"auth">> => #{
            <<"allow_anonymous">> => DisAllow
            %% registry and authentication are required but missed here
            %%
            %% <<"registry">> => <<?PROTO_REG_SERVER_HOST, ?PROTO_REG_REGISTRY_PATH>>,
            %% <<"authentication">> => <<?PROTO_REG_SERVER_HOST, ?PROTO_REG_AUTH_PATH>>
        }
    },
    emqx_utils_maps:deep_merge(raw_jt808_config(), #{<<"proto">> => AuthConfig}).

raw_jt808_config() ->
    #{
        <<"enable">> => true,
        <<"enable_stats">> => true,
        <<"frame">> => #{<<"max_length">> => 8192},
        <<"idle_timeout">> => <<"30s">>,
        <<"max_retry_times">> => 3,
        <<"message_queue_len">> => 10,
        <<"mountpoint">> => <<"jt808/${clientid}/">>,
        <<"proto">> =>
            #{
                <<"dn_topic">> => <<"jt808/${clientid}/${phone}/dn">>,
                <<"up_topic">> => <<"jt808/${clientid}/${phone}/up">>
            },
        <<"retry_interval">> => <<"8s">>
    }.
