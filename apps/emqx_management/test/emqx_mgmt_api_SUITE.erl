%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

t_cluster_query(Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    ct:timetrap({seconds, 120}),
    ListenerConf = fun(Port) ->
        io_lib:format(
            "\n listeners.tcp.default.bind = ~p"
            "\n listeners.ssl.default.enable = false"
            "\n listeners.ws.default.enable = false"
            "\n listeners.wss.default.enable = false",
            [Port]
        )
    end,
    Nodes =
        [Node1, Node2] = emqx_cth_cluster:start(
            [
                {corenode1, #{role => core, apps => [{emqx, ListenerConf(2883)}, emqx_management]}},
                {corenode2, #{role => core, apps => [{emqx, ListenerConf(3883)}, emqx_management]}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
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
        {200, ClientsPage2} = query_clients(Node1, #{<<"page">> => <<"2">>, <<"limit">> => 5}),
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

        %% Scroll past count
        {200, ClientsPage10} = query_clients(Node1, #{<<"page">> => <<"10">>, <<"limit">> => 5}),
        ?assertEqual(
            #{data => [], meta => #{page => 10, limit => 5, count => 20, hasnext => false}},
            ClientsPage10
        ),

        %% Node queries
        {200, ClientsNode2} = query_clients(Node1, #{<<"node">> => Node2}),
        ?assertEqual({200, ClientsNode2}, query_clients(Node2, #{<<"node">> => Node2})),
        ?assertMatch(
            #{page := 1, limit := 100, count := 10},
            maps:get(meta, ClientsNode2)
        ),
        ?assertMatch(10, length(maps:get(data, ClientsNode2))),

        {200, ClientsNode2Page1} = query_clients(Node2, #{<<"node">> => Node2, <<"limit">> => 5}),
        {200, ClientsNode2Page2} = query_clients(Node1, #{
            <<"node">> => Node2, <<"page">> => <<"2">>, <<"limit">> => 5
        }),
        {200, ClientsNode2Page3} = query_clients(Node2, #{
            <<"node">> => Node2, <<"page">> => 3, <<"limit">> => 5
        }),
        {200, ClientsNode2Page4} = query_clients(Node1, #{
            <<"node">> => Node2, <<"page">> => 4, <<"limit">> => 5
        }),
        ?assertEqual(
            GetClientIds(maps:get(data, ClientsNode2)),
            GetClientIds(
                lists:append([
                    maps:get(data, Page)
                 || Page <- [
                        ClientsNode2Page1,
                        ClientsNode2Page2,
                        ClientsNode2Page3,
                        ClientsNode2Page4
                    ]
                ])
            )
        ),

        %% Scroll past count
        {200, ClientsNode2Page10} = query_clients(Node1, #{
            <<"node">> => Node2, <<"page">> => <<"10">>, <<"limit">> => 5
        }),
        ?assertEqual(
            #{data => [], meta => #{page => 10, limit => 5, count => 10, hasnext => false}},
            ClientsNode2Page10
        ),

        %% Query with bad params
        ?assertEqual(
            {400, #{
                code => <<"INVALID_PARAMETER">>,
                message => <<"page_limit_invalid">>
            }},
            query_clients(Node1, #{<<"page">> => -1})
        ),
        ?assertEqual(
            {400, #{
                code => <<"INVALID_PARAMETER">>,
                message => <<"page_limit_invalid">>
            }},
            query_clients(Node1, #{<<"node">> => Node1, <<"page">> => -1})
        ),

        %% Query bad node
        ?assertMatch(
            {500, #{code := <<"NODE_DOWN">>}},
            query_clients(Node1, #{<<"node">> => 'nonode@nohost'})
        ),

        %% exact match can return non-zero total
        {200, ClientsNode1} = query_clients(Node2, #{<<"username">> => <<"corenode1@127.0.0.1">>}),
        ?assertMatch(
            #{count := 10},
            maps:get(meta, ClientsNode1)
        ),

        %% fuzzy searching can't return total
        {200, ClientsFuzzyNode2} = query_clients(Node2, #{<<"like_username">> => <<"corenode2">>}),
        MetaNode2 = maps:get(meta, ClientsFuzzyNode2),
        ?assertNotMatch(#{count := _}, MetaNode2),
        ?assertMatch(#{hasnext := false}, MetaNode2),
        ?assertMatch(10, length(maps:get(data, ClientsFuzzyNode2))),

        _ = lists:foreach(fun(C) -> emqtt:disconnect(C) end, ClientLs1),
        _ = lists:foreach(fun(C) -> emqtt:disconnect(C) end, ClientLs2)
    after
        emqx_cth_cluster:stop(Nodes)
    end.

t_bad_rpc(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    process_flag(trap_exit, true),
    ClientLs1 = [start_emqtt_client(node(), I, 1883) || I <- lists:seq(1, 10)],
    Path = emqx_mgmt_api_test_util:api_path(["clients?limit=2&page=2"]),
    try
        meck:expect(emqx, running_nodes, 0, ['fake@nohost']),
        {error, {_, 500, _}} = emqx_mgmt_api_test_util:request_api(get, Path),
        %% good cop, bad cop
        meck:expect(emqx, running_nodes, 0, [node(), 'fake@nohost']),
        {error, {_, 500, _}} = emqx_mgmt_api_test_util:request_api(get, Path)
    after
        _ = lists:foreach(fun(C) -> emqtt:disconnect(C) end, ClientLs1),
        meck:unload(emqx),
        emqx_cth_suite:stop(Apps)
    end.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

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
