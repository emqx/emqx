%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([ident/1]).

-define(FORMATFUN, {?MODULE, ident}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_management]),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite([emqx_management, emqx_conf]).

init_per_testcase(TestCase, Config) ->
    meck:expect(mria_mnesia, running_nodes, 0, [node()]),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TestCase, Config).

end_per_testcase(TestCase, Config) ->
    meck:unload(mria_mnesia),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TestCase, Config).

t_list_nodes(init, Config) ->
    meck:expect(
        mria_mnesia,
        cluster_nodes,
        fun
            (running) -> [node()];
            (stopped) -> ['stopped@node']
        end
    ),
    Config;
t_list_nodes('end', _Config) ->
    ok.

t_list_nodes(_) ->
    NodeInfos = emqx_mgmt:list_nodes(),
    Node = node(),
    ?assertMatch(
        [
            {Node, #{node := Node, node_status := 'running'}},
            {'stopped@node', #{node := 'stopped@node', node_status := 'stopped'}}
        ],
        NodeInfos
    ).

t_lookup_node(init, Config) ->
    meck:new(os, [passthrough, unstick, no_link]),
    OsType = os:type(),
    meck:expect(os, type, 0, {win32, winME}),
    [{os_type, OsType} | Config];
t_lookup_node('end', Config) ->
    %% We need to restore the original behavior so that rebar3 doesn't crash. If
    %% we'd `meck:unload(os)` or not set `no_link` then `ct` crashes calling
    %% `os` with "The code server called the unloaded module `os'".
    OsType = ?config(os_type, Config),
    meck:expect(os, type, 0, OsType),
    ok.

t_lookup_node(_) ->
    Node = node(),
    ?assertMatch(
        #{node := Node, node_status := 'running', memory_total := 0},
        emqx_mgmt:lookup_node(node())
    ),
    ?assertMatch(
        {error, _},
        emqx_mgmt:lookup_node('fake@nohost')
    ),
    ok.

t_list_brokers(_) ->
    Node = node(),
    ?assertMatch(
        [{Node, #{node := Node, node_status := running, uptime := _}}],
        emqx_mgmt:list_brokers()
    ).

t_lookup_broker(_) ->
    Node = node(),
    ?assertMatch(
        #{node := Node, node_status := running, uptime := _},
        emqx_mgmt:lookup_broker(Node)
    ).

t_get_metrics(_) ->
    Metrics = emqx_mgmt:get_metrics(),
    ?assert(maps:size(Metrics) > 0),
    ?assertMatch(
        Metrics, maps:from_list(emqx_mgmt:get_metrics(node()))
    ).

t_lookup_client(init, Config) ->
    setup_clients(Config);
t_lookup_client('end', Config) ->
    disconnect_clients(Config).

t_lookup_client(_Config) ->
    [{Chan, Info, Stats}] = emqx_mgmt:lookup_client({clientid, <<"client1">>}, ?FORMATFUN),
    ?assertEqual(
        [{Chan, Info, Stats}],
        emqx_mgmt:lookup_client({username, <<"user1">>}, ?FORMATFUN)
    ),
    ?assertEqual([], emqx_mgmt:lookup_client({clientid, <<"notfound">>}, ?FORMATFUN)).

t_kickout_client(init, Config) ->
    process_flag(trap_exit, true),
    setup_clients(Config);
t_kickout_client('end', _Config) ->
    ok.

t_kickout_client(Config) ->
    [C | _] = ?config(clients, Config),
    ok = emqx_mgmt:kickout_client(<<"client1">>),
    receive
        {'EXIT', C, Reason} ->
            ?assertEqual({shutdown, tcp_closed}, Reason);
        Foo ->
            error({unexpected, Foo})
    after 1000 ->
        error(timeout)
    end,
    ?assertEqual({error, not_found}, emqx_mgmt:kickout_client(<<"notfound">>)).

t_list_authz_cache(init, Config) ->
    setup_clients(Config);
t_list_authz_cache('end', Config) ->
    disconnect_clients(Config).

t_list_authz_cache(_) ->
    ?assertNotMatch({error, _}, emqx_mgmt:list_authz_cache(<<"client1">>)),
    ?assertMatch({error, not_found}, emqx_mgmt:list_authz_cache(<<"notfound">>)).

t_list_client_subscriptions(init, Config) ->
    setup_clients(Config);
t_list_client_subscriptions('end', Config) ->
    disconnect_clients(Config).

t_list_client_subscriptions(Config) ->
    [Client | _] = ?config(clients, Config),
    ?assertEqual([], emqx_mgmt:list_client_subscriptions(<<"client1">>)),
    emqtt:subscribe(Client, <<"t/#">>),
    ?assertMatch({_, [{<<"t/#">>, _Opts}]}, emqx_mgmt:list_client_subscriptions(<<"client1">>)),
    ?assertEqual({error, not_found}, emqx_mgmt:list_client_subscriptions(<<"notfound">>)).

%%% helpers
ident(Arg) ->
    Arg.

setup_clients(Config) ->
    {ok, C} = emqtt:start_link([{clientid, <<"client1">>}, {username, <<"user1">>}]),
    {ok, _} = emqtt:connect(C),
    [{clients, [C]} | Config].

disconnect_clients(Config) ->
    Clients = ?config(clients, Config),
    lists:foreach(fun emqtt:disconnect/1, Clients).
