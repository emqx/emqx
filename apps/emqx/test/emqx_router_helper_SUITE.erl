%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_router_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROUTER_HELPER, emqx_router_helper).

all() ->
    [
        {group, routing_schema_v1},
        {group, routing_schema_v2}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {routing_schema_v1, [], [
            {mria_match_delete, [], TCs},
            {fallback, [], TCs}
        ]},
        {routing_schema_v2, [], [
            {mria_match_delete, [], TCs},
            {fallback, [], TCs}
        ]}
    ].

init_per_group(fallback, Config) ->
    ok = mock_mria_match_delete(),
    Config;
init_per_group(mria_match_delete, Config) ->
    Config;
init_per_group(GroupName, Config) ->
    WorkDir = filename:join([?config(priv_dir, Config), ?MODULE, GroupName]),
    AppSpecs = [{emqx, mk_config(GroupName)}],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => WorkDir}),
    Config1 = [{group_name, GroupName}, {group_apps, Apps} | Config],
    init_cluster(Config1).

end_per_group(fallback, _Config) ->
    unmock_mria_match_delete(),
    ok;
end_per_group(mria_match_delete, _Config) ->
    ok;
end_per_group(_GroupName, Config) ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

mk_config(routing_schema_v1) ->
    #{
        config => "broker.routing.storage_schema = v1",
        override_env => [{boot_modules, [broker]}]
    };
mk_config(routing_schema_v2) ->
    #{
        config => "broker.routing.storage_schema = v2",
        override_env => [{boot_modules, [broker]}]
    }.

init_cluster(Config) ->
    GroupName = ?config(group_name, Config),
    WorkDir = filename:join([?config(priv_dir, Config), ?MODULE, GroupName]),
    AppSpecs = [{emqx, mk_config(GroupName)}],
    NodeSpecs = [
        {emqx_router_helper_SUITE1, #{apps => AppSpecs, role => core}},
        {emqx_router_helper_SUITE2, #{apps => AppSpecs, role => core}},
        {emqx_router_helper_SUITE3, #{apps => AppSpecs, role => replicant}}
    ],
    NodeSpecs1 = emqx_cth_cluster:mk_nodespecs(NodeSpecs, #{work_dir => WorkDir}),
    Nodes = emqx_cth_cluster:start(NodeSpecs1),
    [{cluster, Nodes}, {node_specs, NodeSpecs1} | Config].

mock_mria_match_delete() ->
    ok = meck:new(mria, [no_link, passthrough]),
    ok = meck:expect(mria, match_delete, fun(_, _) -> {error, unsupported_otp_version} end).

unmock_mria_match_delete() ->
    ok = meck:unload(mria).

init_per_testcase(TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TestCase, Config).

end_per_testcase(TestCase, Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TestCase, Config).

t_mnesia(_) ->
    ?ROUTER_HELPER ! {membership, testing},
    ?ROUTER_HELPER ! {membership, {mnesia, down, node()}},
    ct:sleep(200).

t_cleanup_membership_mnesia_down(_Config) ->
    Peer = emqx_cth_cluster:node_name(node2),
    emqx_router:add_route(<<"a/b/c">>, Peer),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {_, {ok, _}} = ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {mnesia, down, Peer}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Peer},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_membership_node_down(_Config) ->
    Peer = emqx_cth_cluster:node_name(node3),
    emqx_router:add_route(<<"a/b/c">>, Peer),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {_, {ok, _}} = ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {node, down, Peer}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Peer},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_monitor_node_down(_Config) ->
    [Peer] = emqx_cth_cluster:start_bare_nodes([node4]),
    emqx_router:add_route(<<"a/b/c">>, Peer),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {_, {ok, _}} = ?wait_async_action(
        emqx_cth_cluster:stop([Peer]),
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Peer},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_message(_) ->
    ?ROUTER_HELPER ! testing,
    gen_server:cast(?ROUTER_HELPER, testing),
    gen_server:call(?ROUTER_HELPER, testing).

t_cleanup_routes_race(init, Config) ->
    [Core1, Core2, _Repl] = ?config(cluster, Config),
    lists:foreach(
        fun(N) -> mock_router_helper(N, 4000) end,
        [Core1, Core2]
    ),
    Config;
t_cleanup_routes_race('end', Config) ->
    [Core1, Core2, _Repl] = ?config(cluster, Config),
    lists:foreach(
        fun unmock_router_helper/1,
        [Core1, Core2]
    ).

t_cleanup_routes_race(Config) ->
    ?check_trace(
        begin
            [Core1, Core2, Replicant] = Nodes = ?config(cluster, Config),
            [_, _, ReplSpec] = ?config(node_specs, Config),
            Topic = <<"t/test/must_be_cleaned/", (binary:encode_hex(rand:bytes(8)))/binary>>,
            Topic1 = <<"t/testmust_NOT_be_cleaned/", (binary:encode_hex(rand:bytes(8)))/binary>>,
            ok = rpc:call(Replicant, emqx_router, do_add_route, [Topic]),
            wait_for_route(Replicant, Topic, Replicant),
            {ok, SubRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := emqx_router_helper_cleanup_done}),
                %% Wait for 2 core nodes.
                _NEvents = 2,
                _Timeout = infinity
            ),

            emqx_cth_cluster:restart(Replicant, ReplSpec),
            ok = rpc:call(Replicant, emqx_router, do_add_route, [Topic1]),

            %% wait for Mria route replication,
            %% cannot poll has_route, because late cleanup may wipe it away
            %% if the test is about to fail due to a race
            timer:sleep(1000),
            ?assertMatch({ok, _}, snabbkaffe:receive_events(SubRef)),

            ExpectedRoutes = [
                {Core1, false, true},
                {Core2, false, true},
                {Replicant, false, true}
            ],
            Routes = [
                {N, rpc:call(N, emqx_router, has_route, [Topic, Replicant]),
                    rpc:call(N, emqx_router, has_route, [Topic1, Replicant])}
             || N <- Nodes
            ],
            ?assertEqual(ExpectedRoutes, Routes)
        end,
        []
    ).

mock_router_helper(Node, Delay) ->
    ok = rpc:call(
        Node,
        meck,
        new,
        [emqx_router_helper, [no_link, no_history, unstick, passthrough]]
    ),
    MockFun = fun
        ({nodedown, DownNode}, State) ->
            timer:sleep(Delay),
            meck:passthrough([{nodedown, DownNode}, State]);
        (Msg, State) ->
            meck:passthrough([Msg, State])
    end,
    ok = rpc:call(Node, meck, expect, [emqx_router_helper, handle_info, MockFun]).

unmock_router_helper(Node) ->
    rpc:call(Node, meck, unload, [emqx_router_helper]).

wait_for_route(Node, Topic, Dest) ->
    emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() ->
            rpc:call(Node, emqx_router, has_route, [Topic, Dest])
        end,
        2_000
    ).
