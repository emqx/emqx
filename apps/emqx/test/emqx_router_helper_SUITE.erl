%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROUTER_HELPER, emqx_router_helper).
-define(ROUTE_TAB, emqx_route).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    DistPid =
        case net_kernel:nodename() of
            ignored ->
                %% calling `net_kernel:start' without `epmd'
                %% running will result in a failure.
                emqx_common_test_helpers:start_epmd(),
                {ok, Pid} = net_kernel:start(['test@127.0.0.1', longnames]),
                Pid;
            _ ->
                undefined
        end,
    emqx_common_test_helpers:start_apps([]),
    [{dist_pid, DistPid} | Config].

end_per_suite(Config) ->
    DistPid = ?config(dist_pid, Config),
    case DistPid of
        Pid when is_pid(Pid) ->
            net_kernel:stop();
        _ ->
            ok
    end,
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(TestCase, Config) when
    TestCase =:= t_cleanup_membership_mnesia_down;
    TestCase =:= t_cleanup_membership_node_down;
    TestCase =:= t_cleanup_monitor_node_down
->
    ok = snabbkaffe:start_trace(),
    Slave = emqx_common_test_helpers:start_slave(some_node, []),
    [{slave, Slave} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_cleanup_membership_mnesia_down;
    TestCase =:= t_cleanup_membership_node_down;
    TestCase =:= t_cleanup_monitor_node_down
->
    Slave = ?config(slave, Config),
    emqx_common_test_helpers:stop_slave(Slave),
    mria:transaction(?ROUTE_SHARD, fun() -> mnesia:clear_table(?ROUTE_TAB) end),
    snabbkaffe:stop(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_monitor(_) ->
    ok = emqx_router_helper:monitor({undefined, node()}),
    emqx_router_helper:monitor(undefined).

t_mnesia(_) ->
    ?ROUTER_HELPER ! {mnesia_table_event, {delete, {emqx_routing_node, node()}, undefined}},
    ?ROUTER_HELPER ! {mnesia_table_event, testing},
    ?ROUTER_HELPER ! {mnesia_table_event, {write, {emqx_routing_node, node()}, undefined}},
    ?ROUTER_HELPER ! {membership, testing},
    ?ROUTER_HELPER ! {membership, {mnesia, down, node()}},
    ct:sleep(200).

t_cleanup_membership_mnesia_down(Config) ->
    Slave = ?config(slave, Config),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {mnesia, down, Slave}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_membership_node_down(Config) ->
    Slave = ?config(slave, Config),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {node, down, Slave}},
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_cleanup_monitor_node_down(Config) ->
    Slave = ?config(slave, Config),
    emqx_router:add_route(<<"a/b/c">>, Slave),
    emqx_router:add_route(<<"d/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ?wait_async_action(
        emqx_common_test_helpers:stop_slave(Slave),
        #{?snk_kind := emqx_router_helper_cleanup_done, node := Slave},
        1_000
    ),
    ?assertEqual([<<"d/e/f">>], emqx_router:topics()).

t_message(_) ->
    ?ROUTER_HELPER ! testing,
    gen_server:cast(?ROUTER_HELPER, testing),
    gen_server:call(?ROUTER_HELPER, testing).
