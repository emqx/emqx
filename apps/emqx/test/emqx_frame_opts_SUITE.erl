%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_frame_opts_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(KEY(Zone), {frame_parser_opts, Zone}).
-define(PROTO_VERS, [v3, v4, v5]).

-define(SOCKET_PORT, 21883).
-define(GEN_TCP_FRAME_PORT, 21884).
-define(GEN_TCP_CHUNK_PORT, 21885).
-define(WS_PORT, 21886).

-define(TCP_CASES, [
    t_shared_after_connect,
    t_absent_entry,
    t_client_max_packet_size,
    t_pre_connect_strict_mode,
    t_parse_error
]).

-define(WS_CASES, [
    t_shared_after_connect,
    t_absent_entry,
    t_client_max_packet_size
]).

all() ->
    [
        {group, socket},
        {group, gen_tcp_frame},
        {group, gen_tcp_chunk},
        {group, ws},
        t_build_matches_frame,
        t_global_config_change,
        t_zone_config_change,
        t_zone_removal
    ].

groups() ->
    [
        {socket, [], ?TCP_CASES},
        {gen_tcp_frame, [], ?TCP_CASES},
        {gen_tcp_chunk, [], ?TCP_CASES},
        {ws, [], ?WS_CASES}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, listeners_conf()}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(socket, Config) ->
    [{port, ?SOCKET_PORT}, {transport, tcp}, {conn_mod, emqx_socket_connection} | Config];
init_per_group(gen_tcp_frame, Config) ->
    [{port, ?GEN_TCP_FRAME_PORT}, {transport, tcp}, {conn_mod, emqx_connection} | Config];
init_per_group(gen_tcp_chunk, Config) ->
    [{port, ?GEN_TCP_CHUNK_PORT}, {transport, tcp}, {conn_mod, emqx_connection} | Config];
init_per_group(ws, Config) ->
    [{port, ?WS_PORT}, {transport, ws}, {conn_mod, emqx_ws_connection} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    catch meck:unload(),
    {ok, _} = emqx:update_config([zones], #{}),
    {ok, _} = emqx:update_config([mqtt, max_packet_size], <<"1MB">>),
    {ok, _} = emqx:update_config([mqtt, strict_mode], false),
    %% Restore an entry that a case erased.
    ok = emqx_frame_opts:post_zone_config_update(#{}, emqx_config:get([zones])),
    emqx_common_test_helpers:call_janitor(),
    ok.

listeners_conf() ->
    """
    listeners.tcp.default.enable = false
    listeners.ssl.default.enable = false
    listeners.wss.default.enable = false
    listeners.tcp.socket {
      bind = "127.0.0.1:21883"
      tcp_backend = socket
    }
    listeners.tcp.frame {
      bind = "127.0.0.1:21884"
      tcp_backend = gen_tcp
      parse_unit = frame
    }
    listeners.tcp.chunk {
      bind = "127.0.0.1:21885"
      tcp_backend = gen_tcp
      parse_unit = chunk
    }
    listeners.ws.default.bind = "127.0.0.1:21886"
    """.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
The cached terms match, term for term, what `emqx_frame` builds from the zone
config: before CONNECT and after CONNECT for each protocol version.
""".
t_build_matches_frame(_Config) ->
    {ok, _} = emqx:update_config([mqtt, strict_mode], true),
    FrameOpts = emqx_frame_opts:frame_opts(default),
    ?assertMatch(#{strict_mode := true, expect_connect := true}, FrameOpts),
    #{pre_connect := PreConnect, common := Common} = persistent_term:get(?KEY(default)),
    InitialParseState = emqx_frame:initial_parse_state(FrameOpts),
    ?assertEqual(
        #{
            parse_state => InitialParseState,
            serialize_opts => emqx_frame:initial_serialize_opts(FrameOpts)
        },
        PreConnect
    ),
    ?assertEqual(
        #{
            ProtoVer => #{
                parse_state => emqx_frame:connect_parsed(ProtoVer, InitialParseState),
                serialize_opts => emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE)
            }
         || ProtoVer <- [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]
        },
        Common
    ),
    %% Strict before CONNECT when the zone says so, never after.
    ?assertMatch(#{strict_mode := true}, maps:get(serialize_opts, PreConnect)),
    maps:foreach(
        fun(_ProtoVer, #{serialize_opts := SerializeOpts}) ->
            ?assertMatch(#{strict_mode := false, max_size := ?MAX_PACKET_SIZE}, SerializeOpts)
        end,
        Common
    ).

-doc """
Two connections of each protocol version in the same zone hold the same
shared parse state and serializer options, and messages flow for all three
protocol versions.
""".
t_shared_after_connect(Config) ->
    lists:foreach(
        fun(ProtoVer) ->
            C1 = connect(Config, ProtoVer),
            C2 = connect(Config, ProtoVer),
            V = proto_ver(ProtoVer),
            ?assertEqual(#{parse_state => true, serialize_opts => true}, held_shared(C1, V)),
            ?assertEqual(#{parse_state => true, serialize_opts => true}, held_shared(C2, V)),
            {_ParseState, SerializeOpts} = frame_state(Config, C1),
            ?assertMatch(
                #{version := V, strict_mode := false, max_size := ?MAX_PACKET_SIZE},
                SerializeOpts
            ),
            ok = assert_pubsub(C1, C2),
            ok = emqtt:stop(C1),
            ok = emqtt:stop(C2)
        end,
        ?PROTO_VERS
    ).

-doc """
A connection whose zone has no cache entry builds the same terms locally and
behaves the same as one whose zone has an entry.
""".
t_absent_entry(Config) ->
    Present = [
        {ProtoVer, frame_state(Config, C), C}
     || ProtoVer <- ?PROTO_VERS, C <- [connect(Config, ProtoVer)]
    ],
    _ = persistent_term:erase(?KEY(default)),
    lists:foreach(
        fun({ProtoVer, PresentState, PresentClient}) ->
            C = connect(Config, ProtoVer),
            ?assertEqual(PresentState, frame_state(Config, C)),
            ok = assert_pubsub(C, PresentClient),
            ok = assert_pubsub(PresentClient, C),
            ok = emqtt:stop(C),
            ok = emqtt:stop(PresentClient)
        end,
        Present
    ).

-doc """
An MQTT 5 client that sends `Maximum-Packet-Size` keeps its own serializer
options and does not receive larger packets. A client that does not send it
gets the default limit and the shared serializer options.
""".
t_client_max_packet_size(Config) ->
    Small = connect(Config, v5, [{properties, #{'Maximum-Packet-Size' => 1024}}]),
    Default = connect(Config, v5),
    ?assertEqual(#{parse_state => true, serialize_opts => false}, held_shared(Small, 5)),
    ?assertEqual(#{parse_state => true, serialize_opts => true}, held_shared(Default, 5)),
    {_, SmallSerialize} = frame_state(Config, Small),
    ?assertMatch(#{max_size := 1024, strict_mode := false}, SmallSerialize),
    {_, DefaultSerialize} = frame_state(Config, Default),
    ?assertMatch(#{max_size := ?MAX_PACKET_SIZE}, DefaultSerialize),
    Topic = <<"t/max_packet_size">>,
    {ok, _, [0]} = emqtt:subscribe(Small, Topic, 0),
    {ok, _, [0]} = emqtt:subscribe(Default, Topic, 0),
    Big = binary:copy(<<"x">>, 2048),
    ok = emqtt:publish(Default, Topic, Big, 0),
    ?assertReceive({publish, #{client_pid := Default, payload := Big}}),
    ?assertNotReceive({publish, #{client_pid := Small}}, 500),
    ok = emqtt:publish(Default, Topic, <<"small">>, 0),
    ?assertReceive({publish, #{client_pid := Small, payload := <<"small">>}}),
    ok = emqtt:stop(Small),
    ok = emqtt:stop(Default).

-doc """
Before CONNECT a connection holds the shared pre-CONNECT terms, and the
serializer takes `strict_mode` from the zone. After CONNECT the serializer
has `strict_mode = false`.
""".
t_pre_connect_strict_mode(Config) ->
    {ok, _} = emqx:update_config([mqtt, strict_mode], true),
    #{pre_connect := PreConnect} = persistent_term:get(?KEY(default)),
    #{parse_state := PreParseState, serialize_opts := PreSerialize} = PreConnect,
    ?assertMatch(#{strict_mode := true}, PreSerialize),
    {Sock, Pid} = raw_connect(Config),
    ?assertEqual({PreParseState, PreSerialize}, frame_state(Config, Pid)),
    ?assertEqual(#{parse_state => true, serialize_opts => true}, held_pre_connect(Pid)),
    ConnPkt = ?CONNECT_PACKET(#mqtt_packet_connect{
        proto_ver = ?MQTT_PROTO_V5,
        proto_name = <<"MQTT">>,
        clientid = <<"strict_mode_client">>
    }),
    ok = gen_tcp:send(Sock, emqx_frame:serialize(ConnPkt, ?MQTT_PROTO_V5)),
    {ok, ConnAckBin} = gen_tcp:recv(Sock, 0, 5000),
    ?assertMatch(
        {?CONNACK_PACKET(?RC_SUCCESS), _, _},
        emqx_frame:parse(ConnAckBin, emqx_frame:initial_parse_state(#{version => ?MQTT_PROTO_V5}))
    ),
    {_, PostSerialize} = frame_state(Config, Pid),
    ?assertMatch(#{strict_mode := false, version := ?MQTT_PROTO_V5}, PostSerialize),
    ?assertEqual(#{parse_state => true, serialize_opts => true}, held_shared(Pid, 5)),
    ok = gen_tcp:close(Sock).

-doc """
A CONNECT that fails to parse after its protocol version is read takes the
serializer options for that version through the shared terms, and the
connection closes without a crash.
""".
t_parse_error(Config) ->
    ok = meck:new(emqx_frame_opts, [passthrough, no_link]),
    {Sock, Pid} = raw_connect(Config),
    MRef = monitor(process, Pid),
    %% MQTT 5 CONNECT with the reserved connect flag set.
    ok = gen_tcp:send(Sock, <<16, 10, 0, 4, "MQTT", 5, 1, 0, 60>>),
    ?assertReceive({'DOWN', MRef, process, Pid, _}, 5000),
    ?assertEqual({error, closed}, gen_tcp:recv(Sock, 0, 5000)),
    #{common := #{?MQTT_PROTO_V5 := #{serialize_opts := Shared}}} =
        persistent_term:get(?KEY(default)),
    ?assertMatch(
        [{_, {emqx_frame_opts, connected, [default, ?MQTT_PROTO_V5, _, Shared]}, {_, Shared}}],
        [H || H = {P, {_, connected, _}, _} <- meck:history(emqx_frame_opts), P =:= Pid]
    ).

-doc """
A change to the global `mqtt.max_packet_size` rebuilds the entry of every
zone. An existing connection keeps working with its own terms, and a new
connection shares the new terms.
""".
t_global_config_change(_Config) ->
    Config = [{port, ?SOCKET_PORT}, {transport, tcp}, {conn_mod, emqx_socket_connection}],
    {ok, _} = emqx:update_config([zones], #{<<"other">> => #{}}),
    Old = persistent_term:get(?KEY(default)),
    OldOther = persistent_term:get(?KEY(other)),
    C1 = connect(Config, v5),
    {ok, _} = emqx:update_config([mqtt, max_packet_size], <<"2MB">>),
    assert_zone_entry(default, 2 * 1024 * 1024, Old),
    assert_zone_entry(other, 2 * 1024 * 1024, OldOther),
    assert_after_change(Config, C1, Old).

-doc """
A change to one zone through the `zones` config rebuilds that zone's entry. An
existing connection keeps working with its own terms, and a new connection
shares the new terms.
""".
t_zone_config_change(_Config) ->
    Config = [{port, ?SOCKET_PORT}, {transport, tcp}, {conn_mod, emqx_socket_connection}],
    Old = persistent_term:get(?KEY(default)),
    C1 = connect(Config, v5),
    {ok, _} = emqx:update_config(
        [zones],
        #{<<"default">> => #{<<"mqtt">> => #{<<"max_packet_size">> => <<"3MB">>}}}
    ),
    assert_zone_entry(default, 3 * 1024 * 1024, Old),
    assert_after_change(Config, C1, Old).

-doc "Adding a zone creates its entry, and removing the zone deletes it.".
t_zone_removal(_Config) ->
    ?assertEqual(undefined, persistent_term:get(?KEY(tmpzone), undefined)),
    {ok, _} = emqx:update_config([zones], #{<<"tmpzone">> => #{}}),
    ?assertMatch(#{pre_connect := _, common := _}, persistent_term:get(?KEY(tmpzone))),
    {ok, _} = emqx:update_config([zones], #{}),
    ?assertEqual(undefined, persistent_term:get(?KEY(tmpzone), undefined)),
    ?assertMatch(#{pre_connect := _, common := _}, persistent_term:get(?KEY(default))).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

assert_zone_entry(Zone, MaxSize, Old) ->
    New = persistent_term:get(?KEY(Zone)),
    ?assertNotEqual(Old, New),
    FrameOpts = emqx_frame_opts:frame_opts(Zone),
    ?assertMatch(#{max_size := MaxSize}, FrameOpts),
    #{common := #{?MQTT_PROTO_V5 := #{parse_state := ParseState}}} = New,
    ?assertEqual(
        emqx_frame:connect_parsed(?MQTT_PROTO_V5, emqx_frame:initial_parse_state(FrameOpts)),
        ParseState
    ).

assert_after_change(Config, C1, Old) ->
    #{common := #{?MQTT_PROTO_V5 := #{parse_state := OldParseState}}} = Old,
    %% The existing connection keeps the terms it had. The runtime copied them
    %% into its heap when the old entry was replaced.
    ?assertMatch({OldParseState, _}, frame_state(Config, C1)),
    ?assertEqual(#{parse_state => false, serialize_opts => false}, held_shared(C1, 5)),
    C2 = connect(Config, v5),
    ?assertEqual(#{parse_state => true, serialize_opts => true}, held_shared(C2, 5)),
    ok = assert_pubsub(C1, C2),
    ok = assert_pubsub(C2, C1),
    ok = emqtt:stop(C1),
    ok = emqtt:stop(C2).

connect(Config, ProtoVer) ->
    connect(Config, ProtoVer, []).

connect(Config, ProtoVer, Opts) ->
    ClientId = iolist_to_binary(io_lib:format("c-~p", [erlang:unique_integer([positive])])),
    {ok, C} = emqtt:start_link([
        {clientid, ClientId},
        {host, "127.0.0.1"},
        {port, ?config(port, Config)},
        {proto_ver, ProtoVer}
        | Opts
    ]),
    unlink(C),
    {ok, _} =
        case ?config(transport, Config) of
            tcp -> emqtt:connect(C);
            ws -> emqtt:ws_connect(C)
        end,
    C.

proto_ver(v3) -> ?MQTT_PROTO_V3;
proto_ver(v4) -> ?MQTT_PROTO_V4;
proto_ver(v5) -> ?MQTT_PROTO_V5.

assert_pubsub(Pub, Sub) ->
    Topic = iolist_to_binary(io_lib:format("t/~p", [erlang:unique_integer([positive])])),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    {ok, _} = emqtt:publish(Pub, Topic, <<"hello">>, 1),
    ?assertReceive({publish, #{client_pid := Sub, topic := Topic, payload := <<"hello">>}}),
    ok.

conn_pid(Client) when is_pid(Client) ->
    case proc_lib:initial_call(Client) of
        {emqtt, _, _} ->
            ClientId = proplists:get_value(clientid, emqtt:info(Client)),
            ?retry(50, 20, [_] = emqx_cm:lookup_channels(ClientId)),
            [Pid] = emqx_cm:lookup_channels(ClientId),
            Pid;
        _ ->
            Client
    end.

%% Open a TCP socket to the listener without sending CONNECT, and return it
%% with the connection process that serves it.
raw_connect(Config) ->
    Mod = ?config(conn_mod, Config),
    Before = conn_pids(Mod),
    {ok, Sock} = gen_tcp:connect(
        "127.0.0.1", ?config(port, Config), [binary, {active, false}, {packet, raw}]
    ),
    ?retry(50, 20, [_] = conn_pids(Mod) -- Before),
    [Pid] = conn_pids(Mod) -- Before,
    {Sock, Pid}.

conn_pids(Mod) ->
    [P || P <- erlang:processes(), is_conn_of(Mod, P)].

is_conn_of(Mod, Pid) ->
    case proc_lib:initial_call(Pid) of
        {Mod, _, _} -> true;
        _ -> false
    end.

%% The parse state (unwrapped from `{frame, _}') and the serializer options that
%% the connection process holds.
frame_state(_Config, Client) ->
    Pid = conn_pid(Client),
    Elements = state_elements(sys:get_state(Pid)),
    [ParseState] = [unwrap(E) || E <- Elements, is_parse_state(unwrap(E))],
    [SerializeOpts] = [E || E = #{version := _, strict_mode := _} <- Elements],
    {ParseState, SerializeOpts}.

held_shared(Client, ProtoVer) ->
    held(Client, fun() ->
        #{common := #{ProtoVer := Shared}} = persistent_term:get(?KEY(default)),
        Shared
    end).

held_pre_connect(Pid) ->
    held(Pid, fun() ->
        #{pre_connect := Shared} = persistent_term:get(?KEY(default)),
        Shared
    end).

%% Check inside the connection process whether its state refers to the shared
%% terms themselves, not to equal copies.
held(Client, GetShared) ->
    Pid = conn_pid(Client),
    Self = self(),
    Ref = make_ref(),
    _ = sys:replace_state(Pid, fun(Misc) ->
        #{parse_state := ParseState, serialize_opts := SerializeOpts} = GetShared(),
        Elements = state_elements(Misc),
        Self !
            {Ref, #{
                parse_state => lists:any(
                    fun(E) -> erts_debug:same(unwrap(E), ParseState) end, Elements
                ),
                serialize_opts => lists:any(
                    fun(E) -> erts_debug:same(E, SerializeOpts) end, Elements
                )
            }},
        Misc
    end),
    receive
        {Ref, Result} -> Result
    after 5000 -> error(timeout)
    end.

%% A cowboy websocket process keeps `{State, HandlerState, ParseState}'; the
%% TCP connection processes keep their `#state{}' record.
state_elements({_CowboyState, HandlerState, _ParseState}) when is_tuple(HandlerState) ->
    tuple_to_list(HandlerState);
state_elements(State) ->
    tuple_to_list(State).

unwrap({frame, ParseState}) -> ParseState;
unwrap(Other) -> Other.

is_parse_state(Term) ->
    is_tuple(Term) andalso tuple_size(Term) > 0 andalso element(1, Term) =:= options.
