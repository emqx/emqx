%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TCP_PORT, 20831).
-define(WS_PORT, 20832).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ListenerConf = io_lib:format(
        "listeners.tcp.default.bind = ~b\n"
        "listeners.ws.default.bind = ~b",
        [?TCP_PORT, ?WS_PORT]
    ),
    Apps = emqx_cth_suite:start(
        [{emqx, ListenerConf}],
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

-doc "A TCP MQTT connection is refused while the node is not ready, accepted once ready.".
t_gate_tcp_connection(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:connect/1, #{port => ?TCP_PORT})),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:connect/1, #{port => ?TCP_PORT})).

-doc "A WebSocket MQTT connection is refused while the node is not ready, accepted once ready.".
t_gate_ws_connection(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertMatch({error, _}, try_connect(fun emqtt:ws_connect/1, #{port => ?WS_PORT})),
    ok = emqx_node_readiness:mark_ready(),
    ?assertEqual(ok, try_connect(fun emqtt:ws_connect/1, #{port => ?WS_PORT})).

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
