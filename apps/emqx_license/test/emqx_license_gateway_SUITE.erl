%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_gateway_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-undef(PACKET).
-include_lib("emqx_gateway_coap/include/emqx_coap.hrl").
-include_lib("emqx_gateway_stomp/include/emqx_stomp.hrl").

-define(CRLF, 13, 10).

all() ->
    [{group, Reason} || Reason <- [no_license, quota_exceeded, expired, max_uptime_reached]] ++
        [t_stomp_hook_rejections].

groups() ->
    Tests = [
        t_stomp,
        t_nats,
        t_coap,
        t_lwm2m,
        t_mqttsn,
        t_ocpp,
        t_jt808,
        t_jt808_direct_auth,
        t_jt808_direct_auth_2019
    ],
    [{Reason, [], Tests} || {group, Reason} <- all()].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_gateway,
            emqx_gateway_stomp,
            emqx_gateway_nats,
            emqx_gateway_coap,
            emqx_gateway_lwm2m,
            emqx_gateway_mqttsn,
            emqx_gateway_ocpp,
            emqx_gateway_jt808,
            {emqx_license, #{config => #{license => #{key => default}}}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(Reason, Config) ->
    [{rejection, Reason} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Gateway = gateway(TestCase),
    Transport = transport(Gateway),
    Port = emqx_common_test_helpers:select_free_port(Transport),
    ListenerType = listener_type(Gateway),
    Conf = maps:merge(
        #{
            <<"listeners">> => [
                #{<<"type">> => ListenerType, <<"name">> => <<"default">>, <<"bind">> => Port}
            ]
        },
        gateway_config(Gateway)
    ),
    {ok, _} = emqx_gateway_conf:load_gateway(Gateway, Conf),
    [{gateway, Gateway}, {port, Port} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_gateway_conf:unload_gateway(?config(gateway, Config)).

-doc "STOMP sends a readable ERROR frame on license rejection and connects after recovery.".
t_stomp(Config) ->
    check_rejection(Config, fun stomp_connect/2).

-doc "NATS sends a readable -ERR on license rejection and connects after recovery.".
t_nats(Config) ->
    check_rejection(Config, fun nats_connect/2).

-doc "CoAP returns a readable error on license rejection and connects after recovery.".
t_coap(Config) ->
    check_rejection(Config, fun coap_connect/2).

-doc "LwM2M returns a readable error on license rejection and registers after recovery.".
t_lwm2m(Config) ->
    check_rejection(Config, fun lwm2m_connect/2).

-doc "MQTT-SN maps the license rejection to its congestion code and connects after recovery.".
t_mqttsn(Config) ->
    check_rejection(Config, fun mqttsn_connect/2).

-doc "OCPP returns an HTTP error with a readable reason and upgrades after recovery.".
t_ocpp(Config) ->
    check_rejection(Config, fun ocpp_connect/2).

-doc "JT/T 808 sends a failed authentication response and authenticates after recovery.".
t_jt808(Config) ->
    check_rejection(Config, fun jt808_connect/2).

-doc "JT/T 808 rejects a direct AUTH and accepts registration on the same socket after recovery.".
t_jt808_direct_auth(Config) ->
    check_jt808_direct_auth(Config, 0).

-doc "JT/T 808 uses the 2019 AUTH header for rejection and recovers on the same socket.".
t_jt808_direct_auth_2019(Config) ->
    check_jt808_direct_auth(Config, 1).

check_jt808_direct_auth(Config, ProtoVer) ->
    with_tcp(?config(port, Config), fun(Socket) ->
        with_rejection(?config(rejection, Config), fun() ->
            jt808_auth(Socket, ProtoVer, rejected)
        end),
        jt808_register(Socket, ProtoVer),
        jt808_auth(Socket, ProtoVer, accepted)
    end).

-doc "Connection hook rejections support text, atoms, unknown codes, and compound reasons.".
t_stomp_hook_rejections(Config) ->
    Port = ?config(port, Config),
    Reasons = [
        {not_authorized, <<"not_authorized">>},
        {'access denied', <<"access denied">>},
        {"access denied", <<"access denied">>},
        {<<"access denied">>, <<"access denied">>},
        {[16#62D2, 16#7EDD], unicode:characters_to_binary([16#62D2, 16#7EDD])},
        {255, <<"Unknown error">>},
        {{access_denied, policy}, <<"{access_denied,policy}">>}
    ],
    lists:foreach(
        fun({Reason, Text}) ->
            ok = emqx_hooks:put('client.connect', {?MODULE, reject_connect, [Reason]}, 2001),
            try
                stomp_connect(Port, {rejected, <<"Login Failed: ", Text/binary>>})
            after
                emqx_hooks:del('client.connect', {?MODULE, reject_connect})
            end,
            stomp_connect(Port, accepted)
        end,
        Reasons
    ).

reject_connect(_ConnInfo, _ConnProps, Reason) ->
    {stop, {error, Reason}}.

check_rejection(Config, Connect) ->
    Port = ?config(port, Config),
    Checker = whereis(emqx_license_checker),
    Resources = whereis(emqx_license_resources),
    Connect(Port, accepted),
    with_rejection(?config(rejection, Config), fun() -> Connect(Port, rejected) end),
    check_shutdown_count(?config(gateway, Config), Port),
    ?assertEqual(Checker, whereis(emqx_license_checker)),
    ?assertEqual(Resources, whereis(emqx_license_resources)),
    Connect(Port, accepted).

check_shutdown_count(Gateway, Port) when Gateway =:= stomp; Gateway =:= nats ->
    ListenerId = emqx_gateway_utils:listener_id(Gateway, tcp, default),
    ?retry(
        20,
        100,
        ?assertEqual(
            1,
            proplists:get_value(quota_exceeded, esockd:get_shutdown_count({ListenerId, Port}))
        )
    );
check_shutdown_count(_Gateway, _Port) ->
    ok.

with_rejection(Reason, Fun) ->
    {ok, Limits} = emqx_license_checker:limits(),
    Checker = whereis(emqx_license_checker),
    %% Keep the periodic check from restoring the limits during the connection attempt.
    ok = sys:suspend(Checker),
    try
        _ = sys:replace_state(Checker, fun(State) ->
            set_limits(Reason, Limits),
            State
        end),
        ?assertEqual(
            {stop, {error, ?RC_QUOTA_EXCEEDED}},
            emqx_license:check(#{clientid => clientid()}, #{})
        ),
        Fun()
    after
        _ = sys:replace_state(Checker, fun(State) ->
            true = ets:insert(emqx_license, {limits, Limits}),
            State
        end),
        ok = sys:resume(Checker)
    end,
    ?assertEqual({ok, Limits}, emqx_license_checker:limits()).

set_limits(no_license, _Limits) ->
    true = ets:delete(emqx_license, limits),
    ?assertEqual({error, no_license}, emqx_license_checker:limits());
set_limits(Reason, Limits) ->
    MaxSessions =
        case Reason of
            quota_exceeded -> 0;
            _ -> Reason
        end,
    true = ets:insert(emqx_license, {limits, Limits#{max_sessions := MaxSessions}}).

gateway(t_stomp) -> stomp;
gateway(t_stomp_hook_rejections) -> stomp;
gateway(t_nats) -> nats;
gateway(t_coap) -> coap;
gateway(t_lwm2m) -> lwm2m;
gateway(t_mqttsn) -> mqttsn;
gateway(t_ocpp) -> ocpp;
gateway(t_jt808) -> jt808;
gateway(t_jt808_direct_auth) -> jt808;
gateway(t_jt808_direct_auth_2019) -> jt808.

transport(Gateway) when Gateway =:= coap; Gateway =:= lwm2m; Gateway =:= mqttsn -> udp;
transport(_) -> tcp.

listener_type(ocpp) -> <<"ws">>;
listener_type(Gateway) -> atom_to_binary(transport(Gateway)).

gateway_config(coap) ->
    #{<<"connection_required">> => true};
gateway_config(lwm2m) ->
    Root = emqx_common_test_helpers:proj_root(),
    #{
        <<"xml_dir">> => filename:join([Root, "apps/emqx_gateway_lwm2m/lwm2m_xml"]),
        <<"auto_observe">> => false,
        <<"translators">> => maps:from_list([
            {Name, #{<<"topic">> => <<"lwm2m/", Name/binary>>, <<"qos">> => 0}}
         || Name <- [<<"command">>, <<"response">>, <<"notify">>, <<"register">>, <<"update">>]
        ])
    };
gateway_config(mqttsn) ->
    #{<<"broadcast">> => false};
gateway_config(ocpp) ->
    #{<<"message_format_checking">> => <<"disable">>};
gateway_config(jt808) ->
    #{<<"proto">> => #{<<"auth">> => #{<<"allow_anonymous">> => true}}};
gateway_config(_) ->
    #{}.

clientid() ->
    integer_to_binary(erlang:unique_integer([positive, monotonic])).

with_tcp(Port, Fun) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 5000),
    try
        Fun(Socket)
    after
        gen_tcp:close(Socket)
    end.

with_udp(Port, Fun) ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    try
        Fun(Socket, Port)
    after
        gen_udp:close(Socket)
    end.

stomp_connect(Port, rejected) ->
    stomp_connect(Port, {rejected, <<"Login Failed: Quota exceeded">>});
stomp_connect(Port, Expected) ->
    with_tcp(Port, fun(Socket) ->
        Frame = #stomp_frame{
            command = <<"CONNECT">>,
            headers = [{<<"accept-version">>, <<"1.2">>}, {<<"host">>, <<"localhost">>}]
        },
        ok = gen_tcp:send(Socket, emqx_stomp_frame:serialize_pkt(Frame, #{})),
        {ok, Reply} = recv_stomp(Socket, emqx_stomp_frame:initial_parse_state(#{})),
        case Expected of
            accepted ->
                ?assertEqual(<<"CONNECTED">>, Reply#stomp_frame.command);
            {rejected, Text} ->
                ?assertEqual(<<"ERROR">>, Reply#stomp_frame.command),
                ?assertEqual(Text, Reply#stomp_frame.body),
                ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 5000))
        end
    end).

recv_stomp(Socket, State) ->
    {ok, Data} = gen_tcp:recv(Socket, 0, 5000),
    case emqx_stomp_frame:parse(Data, State) of
        {ok, Frame, _Rest, _Next} -> {ok, Frame};
        {more, Next} -> recv_stomp(Socket, Next)
    end.

nats_connect(Port, Expected) ->
    with_tcp(Port, fun(Socket) ->
        ok = inet:setopts(Socket, [{packet, line}]),
        {ok, <<"INFO ", _/binary>>} = gen_tcp:recv(Socket, 0, 5000),
        ok = gen_tcp:send(Socket, [
            <<"CONNECT ">>, emqx_utils_json:encode(#{verbose => true}), <<?CRLF, "PING", ?CRLF>>
        ]),
        case Expected of
            accepted ->
                ?assertEqual({ok, <<"+OK", ?CRLF>>}, gen_tcp:recv(Socket, 0, 5000)),
                ?assertEqual({ok, <<"PONG", ?CRLF>>}, gen_tcp:recv(Socket, 0, 5000));
            rejected ->
                ?assertEqual(
                    {ok, <<"-ERR Login Failed: Quota exceeded", ?CRLF>>},
                    gen_tcp:recv(Socket, 0, 5000)
                ),
                ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 5000))
        end
    end).

coap_connect(Port, Expected) ->
    Request = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{
            uri_path => [<<"mqtt">>, <<"connection">>],
            uri_query => [<<"clientid=", (clientid())/binary>>]
        }
    },
    check_coap(Port, Request, Expected).

lwm2m_connect(Port, Expected) ->
    Request = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{
            uri_path => [<<"rd">>],
            uri_query => [<<"ep=license-", (clientid())/binary>>, <<"lt=60">>, <<"lwm2m=1">>],
            content_format => 40
        },
        payload = <<"</1>,</2>,</3>">>
    },
    check_coap(Port, Request, Expected).

check_coap(Port, Request, Expected) ->
    with_udp(Port, fun(Socket, ServerPort) ->
        Packet = emqx_coap_frame:serialize_pkt(Request, #{}),
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, ServerPort, Packet),
        {ok, {_, ServerPort, Data}} = gen_udp:recv(Socket, 0, 5000),
        {ok, Reply, <<>>, _} = emqx_coap_frame:parse(Data, #{}),
        ?assertEqual(Request#coap_message.id, Reply#coap_message.id),
        case Expected of
            accepted ->
                ?assertEqual({ok, created}, Reply#coap_message.method);
            rejected ->
                ?assertEqual({error, bad_request}, Reply#coap_message.method),
                ?assertEqual(<<"Login Failed: Quota exceeded">>, Reply#coap_message.payload)
        end
    end).

mqttsn_connect(Port, Expected) ->
    with_udp(Port, fun(Socket, ServerPort) ->
        ClientId = clientid(),
        Length = 6 + byte_size(ClientId),
        ok = gen_udp:send(
            Socket, {127, 0, 0, 1}, ServerPort, <<Length, 4, 4, 1, 0, 0, ClientId/binary>>
        ),
        Code =
            case Expected of
                accepted -> 0;
                rejected -> 1
            end,
        ?assertMatch({ok, {_, ServerPort, <<3, 5, Code>>}}, gen_udp:recv(Socket, 0, 5000))
    end).

ocpp_connect(Port, Expected) ->
    {ok, Conn} = gun:open("127.0.0.1", Port, #{retry => 0}),
    try
        {ok, _} = gun:await_up(Conn, 5000),
        Credentials = base64:encode(<<"guest:guest">>),
        Headers = [{<<"authorization">>, <<"Basic ", Credentials/binary>>}],
        Stream = gun:ws_upgrade(Conn, <<"/ocpp/", (clientid())/binary>>, Headers, #{
            protocols => [{<<"ocpp1.6">>, gun_ws_h}]
        }),
        case Expected of
            accepted ->
                receive
                    {gun_upgrade, Conn, Stream, [<<"websocket">>], _} -> ok
                after 5000 -> ct:fail(websocket_upgrade_timeout)
                end;
            rejected ->
                ?assertMatch({response, _, 400, _}, gun:await(Conn, Stream, 5000)),
                ?assertEqual({ok, <<"Quota exceeded">>}, gun:await_body(Conn, Stream, 5000))
        end
    after
        gun:close(Conn)
    end.

jt808_connect(Port, Expected) ->
    with_tcp(Port, fun(Socket) ->
        jt808_register(Socket, 0),
        jt808_auth(Socket, 0, Expected)
    end).

jt808_register(Socket, ProtoVer) ->
    Register =
        case ProtoVer of
            0 -> <<58:16, 59:16, "examp", 0:160, 0:56, 3, "plate">>;
            1 -> <<58:16, 59:16, "examp", 0:48, 0:240, 0:240, 3, "plate">>
        end,
    ok = gen_tcp:send(Socket, jt808_packet(16#0100, Register, ProtoVer)),
    Reply = recv_jt808(Socket, emqx_jt808_frame:initial_parse_state(#{})),
    ?assertMatch(#{<<"header">> := #{<<"msg_id">> := 16#8100}}, Reply).

jt808_auth(Socket, ProtoVer, Expected) ->
    Auth =
        case ProtoVer of
            0 -> <<"anonymous">>;
            1 -> <<9, "anonymous", "123456789012345", "1.0", 0:136>>
        end,
    ok = gen_tcp:send(Socket, jt808_packet(16#0102, Auth, ProtoVer)),
    Reply = recv_jt808(Socket, emqx_jt808_frame:initial_parse_state(#{})),
    Result =
        case Expected of
            accepted -> 0;
            rejected -> 1
        end,
    #{<<"header">> := Header, <<"body">> := #{<<"data">> := Body}} = Reply,
    ?assertEqual(16#8001, maps:get(<<"msg_id">>, Header)),
    ?assertEqual(ProtoVer, maps:get(<<"proto_ver">>, Header, 0)),
    ?assertEqual(jt808_phone(ProtoVer), maps:get(<<"phone">>, Header)),
    ?assertEqual(<<78:16, 16#0102:16, Result>>, base64:decode(Body)).

jt808_phone(0) -> <<"000123456789">>;
jt808_phone(1) -> <<"00000000000123456789">>.

recv_jt808(Socket, State) ->
    {ok, Data} = gen_tcp:recv(Socket, 0, 5000),
    case emqx_jt808_frame:parse(Data, State) of
        {ok, Frame, <<>>, _Next} -> Frame;
        {more, Next} -> recv_jt808(Socket, Next)
    end.

jt808_packet(Id, Body, ProtoVer) ->
    Data =
        case ProtoVer of
            0 ->
                <<Id:16, (byte_size(Body)):16, 16#000123456789:48, 78:16, Body/binary>>;
            1 ->
                <<Id:16, 0:1, 1:1, 0:1, 0:3, (byte_size(Body)):10, ProtoVer,
                    16#00000000000123456789:80, 78:16, Body/binary>>
        end,
    Checksum = lists:foldl(fun erlang:'bxor'/2, 0, binary_to_list(Data)),
    Escaped = [
        case Byte of
            16#7d -> <<16#7d, 1>>;
            16#7e -> <<16#7d, 2>>;
            _ -> <<Byte>>
        end
     || <<Byte>> <= <<Data/binary, Checksum>>
    ],
    [16#7e, Escaped, 16#7e].
