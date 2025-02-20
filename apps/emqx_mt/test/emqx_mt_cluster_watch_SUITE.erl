%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    meck:new(emqx, [passthrough, no_history, no_link]),
    meck:expect(emqx, running_nodes, fun() -> [] end),
    snabbkaffe:start_trace();
t_node_down({'end', _Config}) ->
    meck:unload(emqx),
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
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Username)),
    %% simulate node down event
    erlang:send(emqx_mt_cluster_watch, {membership, {node, down, node()}}),
    timer:sleep(10),
    ?assertEqual(ignored, gen_server:call(emqx_mt_cluster_watch, dummy, infinity)),
    ok = emqx_mt_state:evict_ccache(),
    ?assertEqual({ok, 0}, emqx_mt:count_clients(Username)),
    ok = emqtt:stop(Pid),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_proc_deleted},
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
