%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_cluster_query(_Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    ct:timetrap({seconds, 120}),
    snabbkaffe:fix_ct_logging(),
    [{Name, Opts}, {Name1, Opts1}] = cluster_specs(),
    Node1 = emqx_common_test_helpers:start_slave(Name, Opts),
    Node2 = emqx_common_test_helpers:start_slave(Name1, Opts1),
    try
        process_flag(trap_exit, true),
        ClientLs1 = [start_emqtt_client(Node1, I, 2883) || I <- lists:seq(1, 10)],
        ClientLs2 = [start_emqtt_client(Node2, I, 3883) || I <- lists:seq(1, 10)],

        %% returned list should be the same regardless of which node is requested
        {200, ClientsAll} = query_clients(Node1, #{}),
        ?assertEqual({200, ClientsAll}, query_clients(Node2, #{})),
        ?assertMatch(
            #{page := 1, limit := 100, count := 20},
            maps:get(meta, ClientsAll)
        ),
        ?assertMatch(20, length(maps:get(data, ClientsAll))),
        %% query the first page, counting in entire cluster
        {200, ClientsPage1} = query_clients(Node1, #{<<"limit">> => 5}),
        ?assertMatch(
            #{page := 1, limit := 5, count := 20},
            maps:get(meta, ClientsPage1)
        ),
        ?assertMatch(5, length(maps:get(data, ClientsPage1))),

        %% assert: AllPage = Page1 + Page2 + Page3 + Page4
        %% !!!Note: this equation requires that the queried tables must be ordered_set
        {200, ClientsPage2} = query_clients(Node1, #{<<"page">> => 2, <<"limit">> => 5}),
        {200, ClientsPage3} = query_clients(Node2, #{<<"page">> => 3, <<"limit">> => 5}),
        {200, ClientsPage4} = query_clients(Node1, #{<<"page">> => 4, <<"limit">> => 5}),
        GetClientIds = fun(L) -> lists:map(fun(#{clientid := Id}) -> Id end, L) end,
        ?assertEqual(
            GetClientIds(maps:get(data, ClientsAll)),
            GetClientIds(
                maps:get(data, ClientsPage1) ++ maps:get(data, ClientsPage2) ++
                    maps:get(data, ClientsPage3) ++ maps:get(data, ClientsPage4)
            )
        ),

        %% exact match can return non-zero total
        {200, ClientsNode1} = query_clients(Node2, #{<<"username">> => <<"corenode1@127.0.0.1">>}),
        ?assertMatch(
            #{count := 10},
            maps:get(meta, ClientsNode1)
        ),

        %% fuzzy searching can't return total
        {200, ClientsNode2} = query_clients(Node2, #{<<"like_username">> => <<"corenode2">>}),
        ?assertMatch(
            #{count := 0},
            maps:get(meta, ClientsNode2)
        ),
        ?assertMatch(10, length(maps:get(data, ClientsNode2))),

        _ = lists:foreach(fun(C) -> emqtt:disconnect(C) end, ClientLs1),
        _ = lists:foreach(fun(C) -> emqtt:disconnect(C) end, ClientLs2)
    after
        emqx_common_test_helpers:stop_slave(Node1),
        emqx_common_test_helpers:stop_slave(Node2)
    end,
    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

cluster_specs() ->
    Specs =
        %% default listeners port
        [
            {core, corenode1, #{listener_ports => [{tcp, 2883}]}},
            {core, corenode2, #{listener_ports => [{tcp, 3883}]}}
        ],
    CommOpts =
        [
            {env, [{emqx, boot_modules, all}]},
            {apps, []},
            {conf, [
                {[listeners, ssl, default, enabled], false},
                {[listeners, ws, default, enabled], false},
                {[listeners, wss, default, enabled], false}
            ]}
        ],
    emqx_common_test_helpers:emqx_cluster(
        Specs,
        CommOpts
    ).

start_emqtt_client(Node0, N, Port) ->
    Node = atom_to_binary(Node0),
    ClientId = iolist_to_binary([Node, "-", integer_to_binary(N)]),
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {username, Node}, {port, Port}]),
    {ok, _} = emqtt:connect(C),
    C.

query_clients(Node, Qs0) ->
    Qs = maps:merge(
        #{<<"page">> => 1, <<"limit">> => 100},
        Qs0
    ),
    rpc:call(Node, emqx_mgmt_api_clients, clients, [get, #{query_string => Qs}]).
