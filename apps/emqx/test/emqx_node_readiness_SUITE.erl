%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TCP_PORT, 20831).
-define(WS_PORT, 20832).
-define(QUIC_PORT, 20833).
-define(TCP_SOCKET_PORT, 20834).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = emqx_common_test_helpers:gen_ca(PrivDir, "ca"),
    _ = emqx_common_test_helpers:gen_host_cert("server", "ca", PrivDir, #{}),
    ListenerConf = io_lib:format(
        "listeners.tcp.default.bind = ~b\n"
        "listeners.tcp.sock.bind = ~b\n"
        "listeners.tcp.sock.tcp_backend = socket\n"
        "listeners.ws.default.bind = ~b\n"
        "listeners.quic.default {\n"
        "  enable = true\n"
        "  bind = ~b\n"
        "  ssl_options {\n"
        "    cacertfile = \"~ts\"\n"
        "    certfile = \"~ts\"\n"
        "    keyfile = \"~ts\"\n"
        "  }\n"
        "}",
        [
            ?TCP_PORT,
            ?TCP_SOCKET_PORT,
            ?WS_PORT,
            ?QUIC_PORT,
            filename:join(PrivDir, "ca.pem"),
            filename:join(PrivDir, "server.pem"),
            filename:join(PrivDir, "server.key")
        ]
    ),
    Apps = emqx_cth_suite:start(
        [quicer, {emqx, ListenerConf}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_node_readiness:mark_ready().

-doc "The readiness flag defaults to true and toggles with mark_not_ready/mark_ready.".
t_readiness_flag(_Config) ->
    _ = persistent_term:erase({emqx_node_readiness, ready}),
    ?assert(emqx_node_readiness:is_ready()),
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertNot(emqx_node_readiness:is_ready()),
    ok = emqx_node_readiness:mark_ready(),
    ?assert(emqx_node_readiness:is_ready()).

-doc """
A TCP MQTT connection is refused while the node is not ready, accepted once
ready.  The refusal is recorded in the listener's shutdown counter.
""".
t_gate_tcp_connection(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:connect/1, #{port => ?TCP_PORT})),
    Bind = emqx_config:get([listeners, tcp, default, bind]),
    ?retry(
        100,
        10,
        ?assertMatch(
            {node_not_ready, _},
            lists:keyfind(
                node_not_ready, 1, emqx_listeners:shutdown_count(<<"tcp:default">>, Bind)
            )
        )
    ),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:connect/1, #{port => ?TCP_PORT})).

-doc """
A TCP MQTT connection on a listener with `tcp_backend = socket` is refused
while the node is not ready, accepted once ready.  The refusal is recorded
in the listener's shutdown counter.
""".
t_gate_tcp_socket_backend_connection(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:connect/1, #{port => ?TCP_SOCKET_PORT})),
    Bind = emqx_config:get([listeners, tcp, sock, bind]),
    ?retry(
        100,
        10,
        ?assertMatch(
            {node_not_ready, _},
            lists:keyfind(
                node_not_ready, 1, emqx_listeners:shutdown_count(<<"tcp:sock">>, Bind)
            )
        )
    ),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:connect/1, #{port => ?TCP_SOCKET_PORT})).

-doc "A WebSocket MQTT connection is refused while the node is not ready, accepted once ready.".
t_gate_ws_connection(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:ws_connect/1, #{port => ?WS_PORT})),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:ws_connect/1, #{port => ?WS_PORT})).

-doc "A QUIC MQTT connection is refused while the node is not ready, accepted once ready.".
t_gate_quic_connection(_Config) ->
    QuicOpts = #{
        port => ?QUIC_PORT,
        ssl => true,
        ssl_opts => [{verify, verify_none}]
    },
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:quic_connect/1, QuicOpts)),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:quic_connect/1, QuicOpts)).

-doc "A cluster join is refused on both the joining and the target node while not ready.".
t_gate_cluster_join(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, emqx_cluster:can_i_join(node())),
    ?assertMatch(
        {error, "This node has not fully booted" ++ _},
        emqx_cluster:join('nosuchnode@127.0.0.1')
    ),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, emqx_cluster:can_i_join(node())).

try_connect(ConnFun, Opts0) ->
    Opts = maps:merge(#{host => "127.0.0.1", connect_timeout => 5}, Opts0),
    {ok, Client} = emqtt:start_link(Opts),
    true = erlang:unlink(Client),
    case ConnFun(Client) of
        {ok, _} ->
            ok = emqtt:disconnect(Client);
        {error, Reason} ->
            catch emqtt:stop(Client),
            {error, Reason}
    end.
