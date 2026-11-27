%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_banned_tarpit_SUITE).

-moduledoc """
The CONNECT tarpit for banned clients, `banned.tarpit`, on each listener type.

The cases use a raw socket, not an MQTT client, so that they see each byte
that the server sends before it closes the connection. On ws and wss, this
includes a WebSocket close frame.
""".

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-define(DELAY_MS, 1000).
%% How much later than the delay the close may arrive.
-define(LATE_MS, 1000).
%% Longer than any delay in this suite.
-define(RECV_TIMEOUT_MS, 10000).
-define(HIBERNATED, {current_function, {erlang, hibernate, 3}}).
-define(QUIC_PORT, 24568).

all() ->
    [{group, G} || G <- [tcp, ssl, tcpsocket, ws, wss]] ++ quic_groups().

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    TCs = [TC || TC <- All, not lists:prefix("t_quic_", atom_to_list(TC))],
    QuicTCs = All -- TCs,
    [{G, [], TCs} || G <- [tcp, ssl, tcpsocket, ws, wss]] ++ [{quic, [], QuicTCs}].

-ifndef(BUILD_WITHOUT_QUIC).
quic_groups() -> [{group, quic}].
-else.
quic_groups() -> [].
-endif.

init_per_group(Group, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, conf(Group)}],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [{apps, Apps}, {group, Group} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = emqx_hooks:del('client.connack', {?MODULE, on_client_connack}),
    ok = emqx_banned:clear(),
    ok = emqx_config:put([banned, tarpit], #{delay => 0, max => 1000}),
    ok = await_tarpit_count(0).

conf(tcpsocket) ->
    "listeners.tcp.default.tcp_backend = socket";
conf(quic) ->
    emqx_utils:format(
        """
        listeners.quic.default {
          enable = true
          bind = "127.0.0.1:~p"
        }
        """,
        [?QUIC_PORT]
    );
conf(_Group) ->
    "".

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-doc "The tarpit is off by default: a banned client gets CONNACK \"Banned\" at once.".
t_off_by_default(Config) ->
    ?assertMatch(#{delay := 0}, emqx_config:get([banned, tarpit])),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    Sock = connect(Config),
    ok = send_connect(Sock, ClientId, <<"u">>, Config),
    ?assertEqual(?RC_BANNED, recv_connack_rc(Sock, Config)),
    close(Sock, Config).

-doc """
A client banned by client ID gets no byte from the server, and the server
closes the connection after the delay. The `client.connack` hook runs with
reason `banned`, `client.banned` counts the client, and the connection
process hibernates.
""".
t_tarpit_clientid(Config) ->
    ok = emqx_hooks:add('client.connack', {?MODULE, on_client_connack, [self()]}, 0),
    ok = set_tarpit(<<"1s">>, 1000),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    Banned0 = emqx_metrics:val_global('client.banned'),
    Sock = connect(Config),
    T0 = now_ms(),
    ok = send_connect(Sock, ClientId, <<"u">>, Config),
    receive
        {client_connack, Reason} -> ?assertEqual(banned, Reason)
    after 5000 ->
        ct:fail(no_client_connack_hook)
    end,
    ?assertEqual(Banned0 + 1, emqx_metrics:val_global('client.banned')),
    Pid = await_tarpitted_pid(),
    ?assertEqual(1, emqx_banned:info(tarpitted)),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = assert_closed_after_delay(Sock, T0, Config),
    ok = await_tarpit_count(0).

-doc "A client banned by username is held in the tarpit.".
t_tarpit_username(Config) ->
    ok = set_tarpit(<<"1s">>, 1000),
    Username = atom_to_binary(?FUNCTION_NAME),
    ok = ban(username, Username),
    Sock = connect(Config),
    T0 = now_ms(),
    ok = send_connect(Sock, <<"c-", Username/binary>>, Username, Config),
    ok = assert_closed_after_delay(Sock, T0, Config).

-doc "A client banned by peer host is held in the tarpit.".
t_tarpit_peerhost(Config) ->
    ok = set_tarpit(<<"1s">>, 1000),
    ok = ban(peerhost, <<"127.0.0.1">>),
    Sock = connect(Config),
    T0 = now_ms(),
    ok = send_connect(Sock, atom_to_binary(?FUNCTION_NAME), <<"u">>, Config),
    ok = assert_closed_after_delay(Sock, T0, Config).

-doc """
Packets that follow CONNECT, in the same read or later, get no reply, and do
not end the tarpit early.
""".
t_tarpit_ignores_more_packets(Config) ->
    ok = set_tarpit(<<"1s">>, 1000),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    Sock = connect(Config),
    T0 = now_ms(),
    Connect = connect_bytes(ClientId, <<"u">>),
    Ping = emqx_frame:serialize(?PACKET(?PINGREQ)),
    Sub = emqx_frame:serialize(?SUBSCRIBE_PACKET(1, [{<<"t">>, ?DEFAULT_SUBOPTS}]), ?MQTT_PROTO_V5),
    ok = send_bytes(Sock, [Connect, Ping], Config),
    _ = await_tarpit_count(1),
    ok = send_bytes(Sock, [Ping, Sub], Config),
    ok = assert_closed_after_delay(Sock, T0, Config).

-doc """
When `max` clients are held, the next banned client gets CONNACK "Banned" at
once. The slot is free again after the held connection closes.
""".
t_max_reached(Config) ->
    ok = set_tarpit(<<"1s">>, 1),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    Sock1 = connect(Config),
    T0 = now_ms(),
    ok = send_connect(Sock1, ClientId, <<"u">>, Config),
    ok = await_tarpit_count(1),
    Sock2 = connect(Config),
    ok = send_connect(Sock2, ClientId, <<"u">>, Config),
    ?assertEqual(?RC_BANNED, recv_connack_rc(Sock2, Config)),
    close(Sock2, Config),
    ok = assert_closed_after_delay(Sock1, T0, Config),
    ok = await_tarpit_count(0),
    Sock3 = connect(Config),
    T3 = now_ms(),
    ok = send_connect(Sock3, ClientId, <<"u">>, Config),
    ok = assert_closed_after_delay(Sock3, T3, Config).

-doc """
A held connection that is killed, so that it runs no `terminate`, frees its
slot.
""".
t_killed_frees_slot(Config) ->
    ok = set_tarpit(<<"1m">>, 1000),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    Sock = connect(Config),
    ok = send_connect(Sock, ClientId, <<"u">>, Config),
    ok = await_tarpit_count(1),
    Pid = await_tarpitted_pid(),
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MRef, process, Pid, killed} -> ok
    after 5000 ->
        ct:fail(not_killed)
    end,
    ok = await_tarpit_count(0),
    close(Sock, Config).

-doc "A schema check rejects a delay above the maximum.".
t_delay_upper_bound(_Config) ->
    ?assertMatch({ok, _}, emqx:update_config([banned, tarpit, delay], <<"5m">>)),
    ?assertMatch({error, _}, emqx:update_config([banned, tarpit, delay], <<"301s">>)),
    ok.

-doc "A banned QUIC client gets CONNACK \"Banned\" while the tarpit is enabled.".
t_quic_not_tarpitted(_Config) ->
    ok = set_tarpit(<<"1s">>, 1000),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = ban(clientid, ClientId),
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {port, ?QUIC_PORT}, {proto_ver, v5}]),
    unlink(C),
    T0 = now_ms(),
    ?assertMatch({error, {banned, _}}, emqtt:quic_connect(C)),
    ?assert(now_ms() - T0 < ?DELAY_MS),
    ?assertEqual(0, emqx_banned:info(tarpitted)).

%%--------------------------------------------------------------------
%% Hook
%%--------------------------------------------------------------------

on_client_connack(_ConnInfo, Reason, Props, TestPid) ->
    TestPid ! {client_connack, Reason},
    {ok, Props}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

set_tarpit(Delay, Max) ->
    {ok, _} = emqx:update_config([banned, tarpit], #{<<"delay">> => Delay, <<"max">> => Max}),
    ok.

ban(Kind, Who) ->
    {ok, _} = emqx_banned:create(#banned{
        who = emqx_banned:who(Kind, Who),
        by = <<"tarpit suite">>,
        reason = <<"test">>,
        at = erlang:system_time(second),
        until = erlang:system_time(second) + 600
    }),
    ok.

now_ms() ->
    erlang:monotonic_time(millisecond).

%% The slot owner monitors a held connection after a message from it.
await_tarpitted_pid() ->
    await_tarpitted_pid(50).

await_tarpitted_pid(Retries) ->
    {monitors, Monitors} = process_info(whereis(emqx_banned_tarpit), monitors),
    case [Pid || {process, Pid} <- Monitors] of
        [Pid] ->
            Pid;
        _ when Retries > 0 ->
            ct:sleep(20),
            await_tarpitted_pid(Retries - 1);
        Pids ->
            ct:fail({unexpected_tarpitted_pids, Pids})
    end.

await_tarpit_count(N) ->
    await_tarpit_count(N, 50).

await_tarpit_count(N, 0) ->
    ?assertEqual(N, emqx_banned:info(tarpitted));
await_tarpit_count(N, Retries) ->
    case emqx_banned:info(tarpitted) of
        N ->
            ok;
        _ ->
            ct:sleep(100),
            await_tarpit_count(N, Retries - 1)
    end.

await_hibernated(Pid) ->
    await_hibernated(Pid, 50).

await_hibernated(Pid, 0) ->
    process_info(Pid, current_function);
await_hibernated(Pid, Retries) ->
    case process_info(Pid, current_function) of
        ?HIBERNATED = Hibernated ->
            Hibernated;
        _ ->
            ct:sleep(20),
            await_hibernated(Pid, Retries - 1)
    end.

%% The server sends no byte, also no WebSocket close frame, and closes the
%% connection after the delay.
assert_closed_after_delay(Sock, T0, Config) ->
    Result = recv(Sock, Config),
    Elapsed = now_ms() - T0,
    ?assertEqual({error, closed}, Result),
    ?assert(Elapsed >= ?DELAY_MS, #{elapsed_ms => Elapsed}),
    ?assert(Elapsed < ?DELAY_MS + ?LATE_MS, #{elapsed_ms => Elapsed}),
    ok.

connect_bytes(ClientId, Username) ->
    emqx_frame:serialize(
        ?CONNECT_PACKET(#mqtt_packet_connect{
            proto_name = <<"MQTT">>,
            proto_ver = ?MQTT_PROTO_V5,
            clientid = ClientId,
            username = Username,
            keepalive = 60
        }),
        ?MQTT_PROTO_V5
    ).

send_connect(Sock, ClientId, Username, Config) ->
    send_bytes(Sock, [connect_bytes(ClientId, Username)], Config).

recv_connack_rc(Sock, Config) ->
    {ok, Bin} = recv(Sock, Config),
    ParseState = emqx_frame:initial_parse_state(#{version => ?MQTT_PROTO_V5}),
    {?CONNACK_PACKET(RC), _Rest, _} = emqx_frame:parse(Bin, ParseState),
    RC.

%%--------------------------------------------------------------------
%% Raw transport: tcp and ssl send MQTT bytes on the socket, ws and wss send
%% them in masked WebSocket binary frames after an HTTP upgrade.

transport(Config) ->
    case ?config(group, Config) of
        tcp -> {gen_tcp, tcp};
        tcpsocket -> {gen_tcp, tcp};
        ssl -> {ssl, ssl};
        ws -> {gen_tcp, ws};
        wss -> {ssl, wss}
    end.

connect(Config) ->
    {Mod, Type} = transport(Config),
    Port = listener_port(Type),
    Opts = [binary, {active, false}, {packet, raw}],
    {ok, Sock} =
        case Mod of
            gen_tcp -> gen_tcp:connect("127.0.0.1", Port, Opts, 5000);
            ssl -> ssl:connect("127.0.0.1", Port, [{verify, verify_none} | Opts], 5000)
        end,
    ok = maybe_ws_upgrade(Type, Mod, Sock, Port),
    Sock.

listener_port(Type) ->
    case emqx_config:get_listener_conf(Type, default, [bind]) of
        {_Ip, Port} -> Port;
        Port when is_integer(Port) -> Port
    end.

maybe_ws_upgrade(Type, Mod, Sock, Port) when Type =:= ws; Type =:= wss ->
    Req = [
        [Line, "\r\n"]
     || Line <- [
            "GET /mqtt HTTP/1.1",
            ["Host: 127.0.0.1:", integer_to_list(Port)],
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
            "Sec-WebSocket-Version: 13",
            "Sec-WebSocket-Protocol: mqtt",
            ""
        ]
    ],
    ok = Mod:send(Sock, Req),
    Resp = recv_http_response(Mod, Sock, <<>>),
    ?assertMatch(<<"HTTP/1.1 101", _/binary>>, Resp),
    ok;
maybe_ws_upgrade(_Type, _Mod, _Sock, _Port) ->
    ok.

recv_http_response(Mod, Sock, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        {Pos, 4} ->
            %% The server sends nothing after the upgrade response before
            %% CONNECT.
            ?assertEqual(byte_size(Acc), Pos + 4),
            Acc;
        nomatch ->
            {ok, Bin} = Mod:recv(Sock, 0, 5000),
            recv_http_response(Mod, Sock, <<Acc/binary, Bin/binary>>)
    end.

send_bytes(Sock, IoData, Config) ->
    case transport(Config) of
        {Mod, Type} when Type =:= ws; Type =:= wss ->
            Mod:send(Sock, cow_ws:masked_frame({binary, iolist_to_binary(IoData)}, #{}));
        {Mod, _} ->
            Mod:send(Sock, IoData)
    end.

%% Return the payload of the first WebSocket frame on ws and wss.
recv(Sock, Config) ->
    case transport(Config) of
        {Mod, Type} when Type =:= ws; Type =:= wss ->
            case Mod:recv(Sock, 0, ?RECV_TIMEOUT_MS) of
                {ok, Bin} ->
                    {binary, _, _, _, _, Rest} = cow_ws:parse_header(Bin, #{}, undefined),
                    {ok, Rest};
                Error ->
                    Error
            end;
        {Mod, _} ->
            Mod:recv(Sock, 0, ?RECV_TIMEOUT_MS)
    end.

close(Sock, Config) ->
    {Mod, _} = transport(Config),
    _ = Mod:close(Sock),
    ok.
