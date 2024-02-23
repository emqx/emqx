%%--------------------------------------------------------------------
%% Copyright (c) 2019-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    [{group_name, GroupName}, {group_apps, Apps} | Config].

end_per_group(fallback, _Config) ->
    unmock_mria_match_delete(),
    ok;
end_per_group(mria_match_delete, _Config) ->
    ok;
end_per_group(_GroupName, Config) ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config)).

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

mock_mria_match_delete() ->
    ok = meck:new(mria, [no_link, passthrough]),
    ok = meck:expect(mria, match_delete, fun(_, _) -> {error, unsupported_otp_version} end).

unmock_mria_match_delete() ->
    ok = meck:unload(mria).

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    ok.

t_monitor(_) ->
    ok = emqx_router_helper:monitor({undefined, node()}),
    emqx_router_helper:monitor(undefined).

t_mnesia(_) ->
    ?ROUTER_HELPER ! {mnesia_table_event, {delete, {?ROUTING_NODE, node()}, undefined}},
    ?ROUTER_HELPER ! {mnesia_table_event, testing},
    ?ROUTER_HELPER ! {mnesia_table_event, {write, {?ROUTING_NODE, node()}, undefined}},
    ?ROUTER_HELPER ! {membership, testing},
    ?ROUTER_HELPER ! {membership, {mnesia, down, node()}},
    ct:sleep(200).

t_cleanup_membership_mnesia_down(_Config) ->
    Slave = emqx_cth_cluster:node_name(node2),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {mnesia, down, Slave}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_membership_node_down(_Config) ->
    Slave = emqx_cth_cluster:node_name(node3),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {node, down, Slave}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_monitor_node_down(_Config) ->
    [Slave] = emqx_cth_cluster:start_bare_nodes([node4]),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        emqx_cth_cluster:stop([Slave]),
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_message(_) ->
    ?ROUTER_HELPER ! testing,
    gen_server:cast(?ROUTER_HELPER, testing),
    gen_server:call(?ROUTER_HELPER, testing).
