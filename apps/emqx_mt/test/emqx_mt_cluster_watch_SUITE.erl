%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_cluster_watch_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CLIENTID, iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME))).
-define(USERNAME, iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            emqx_mt
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}),
    Config.

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}),
    ok.

t_node_down({init, _Config}) ->
    snabbkaffe:start_trace();
t_node_down({'end', _Config}) ->
    snabbkaffe:stop();
t_node_down(_Config) ->
    ClientId = ?CLIENTID,
    Username = ?USERNAME,
    Pid = connect(ClientId, Username),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual(1, emqx_mt:count_clients(Username)),
    %% simulate node down
    erlang:send(emqx_mt_cluster_watch, {membership, {node, down, node()}}),
    ?assertMatch(
        {ok, #{result := ok, took := _}},
        ?block_until(
            #{?snk_kind := multi_tenant_node_clear_done},
            3000
        )
    ),
    ?assertEqual(0, emqx_mt:count_clients(Username)),
    ok = emqtt:stop(Pid),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_deleted},
            3000
        )
    ),
    ok.

connect(ClientId, Username) ->
    Opts = [{clientid, ClientId}, {username, Username}],
    {ok, Pid} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Pid),
    monitor(process, Pid),
    unlink(Pid),
    Pid.
