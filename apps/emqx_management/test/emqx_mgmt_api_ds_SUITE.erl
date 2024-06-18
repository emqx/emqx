%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_util, [api_path/1, request_api/2, request_api_with_body/3]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {group, cluster},
        AllTCs -- cluster_tests()
    ].

groups() ->
    [{cluster, [], cluster_tests()}].

cluster_tests() ->
    [
        t_get_db_node_down
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, "durable_sessions.enable = true"},
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(cluster = Group, Config) ->
    AppSpecs = [
        {emqx, "durable_sessions.enable = true\n"},
        emqx_management
    ],
    Dashboard = emqx_mgmt_api_test_util:emqx_dashboard(
        %% Using different port so it doesn't clash with the master node's port.
        "dashboard.listeners.http { enable = true, bind = 18084 }"
    ),
    Cluster = [
        {mgmt_api_ds_SUITE1, #{role => core, apps => AppSpecs ++ [Dashboard]}},
        {mgmt_api_ds_SUITE2, #{role => core, apps => AppSpecs}}
    ],
    ClusterOpts = #{work_dir => emqx_cth_suite:work_dir(Group, Config)},
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(Cluster, ClusterOpts),
    Nodes = emqx_cth_cluster:start(Cluster, ClusterOpts),
    [
        {nodes, Nodes},
        {node_specs, NodeSpecs},
        {cluster_opts, ClusterOpts}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    Nodes = ?config(nodes, Config),
    emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

t_get_sites(_) ->
    Path = api_path(["ds", "sites"]),
    {ok, Response} = request_api(get, Path),
    ?assertEqual(
        [emqx_ds_replication_layer_meta:this_site()],
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_storages(_) ->
    Path = api_path(["ds", "storages"]),
    {ok, Response} = request_api(get, Path),
    ?assertEqual(
        [<<"messages">>],
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_site(_) ->
    %% Unknown sites must result in error 404:
    Path404 = api_path(["ds", "sites", "unknown_site"]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, Path404)
    ),
    %% Valid path:
    Path = api_path(["ds", "sites", emqx_ds_replication_layer_meta:this_site()]),
    {ok, Response} = request_api(get, Path),
    ThisNode = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"node">> := ThisNode,
            <<"up">> := true,
            <<"shards">> :=
                [
                    #{
                        <<"storage">> := <<"messages">>,
                        <<"id">> := _,
                        <<"status">> := <<"up">>
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_db(_) ->
    %% Unknown DBs must result in error 400 (since the DS parameter is an enum):
    Path400 = api_path(["ds", "storages", "unknown_ds"]),
    ?assertMatch(
        {error, {_, 400, _}},
        request_api(get, Path400)
    ),
    %% Valid path:
    Path = api_path(["ds", "storages", "messages"]),
    {ok, Response} = request_api(get, Path),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    ?assertMatch(
        #{
            <<"name">> := <<"messages">>,
            <<"shards">> :=
                [
                    #{
                        <<"id">> := _,
                        <<"replicas">> :=
                            [
                                #{
                                    <<"site">> := ThisSite,
                                    <<"status">> := <<"up">>
                                }
                                | _
                            ]
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Response)
    ).

%% Smoke test that checks the status of down replica nodes.
t_get_db_node_down(Config) ->
    [_N1, N2] = ?config(nodes, Config),
    [_N1Spec, N2Spec] = ?config(node_specs, Config),
    Path = api_path_cluster(["ds", "storages", "messages"], Config),
    Params = "",

    {Status1, Res1} = simple_request(get, Path, Params),
    ?assertEqual(200, Status1),
    #{<<"shards">> := Shards1} = Res1,
    Replicas1 = [Replica || #{<<"replicas">> := Replicas} <- Shards1, Replica <- Replicas],
    ?assert(
        lists:all(fun(#{<<"status">> := Status}) -> Status =:= <<"up">> end, Replicas1),
        #{replicas => Replicas1}
    ),

    %% Now make one of the nodes down.
    StoppedSite = ?ON(N2, emqx_ds_replication_layer_meta:this_site()),
    ok = emqx_cth_cluster:stop_node(N2),
    ct:sleep(1_000),

    {Status2, Res2} = simple_request(get, Path, Params),
    ?assertEqual(200, Status2),
    #{<<"shards">> := Shards2} = Res2,
    Replicas2 = [Replica || #{<<"replicas">> := Replicas} <- Shards2, Replica <- Replicas],
    ?assert(
        lists:all(
            fun(#{<<"site">> := Site, <<"status">> := Status}) ->
                case Site =:= StoppedSite of
                    true -> Status =:= <<"down">>;
                    false -> Status =:= <<"up">>
                end
            end,
            Replicas2
        ),
        #{
            replicas => Replicas2,
            stopped_site => StoppedSite
        }
    ),

    %% Back online
    emqx_cth_cluster:restart(N2Spec),
    ct:sleep(1_000),

    {Status3, Res3} = simple_request(get, Path, Params),
    ?assertEqual(200, Status3),
    #{<<"shards">> := Shards3} = Res3,
    Replicas3 = [Replica || #{<<"replicas">> := Replicas} <- Shards3, Replica <- Replicas],
    ?assert(
        lists:all(fun(#{<<"status">> := Status}) -> Status =:= <<"up">> end, Replicas3),
        #{replicas => Replicas3}
    ),

    ok.

t_get_replicas(_) ->
    %% Unknown DBs must result in error 400 (since the DS parameter is an enum):
    Path400 = api_path(["ds", "storages", "unknown_ds", "replicas"]),
    ?assertMatch(
        {error, {_, 400, _}},
        request_api(get, Path400)
    ),
    %% Valid path:
    Path = api_path(["ds", "storages", "messages", "replicas"]),
    {ok, Response} = request_api(get, Path),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    ?assertEqual(
        [ThisSite],
        emqx_utils_json:decode(Response)
    ).

t_put_replicas(_) ->
    Path = api_path(["ds", "storages", "messages", "replicas"]),
    %% Error cases:
    ?assertMatch(
        {ok, 400, #{<<"message">> := <<"Unknown sites: invalid_site">>}},
        parse_error(request_api_with_body(put, Path, [<<"invalid_site">>]))
    ),
    %% Success case:
    ?assertMatch(
        {ok, 202, <<"OK">>},
        request_api_with_body(put, Path, [emqx_ds_replication_layer_meta:this_site()])
    ).

t_join(_) ->
    Path400 = api_path(["ds", "storages", "messages", "replicas", "unknown_site"]),
    ?assertMatch(
        {error, {_, 400, _}},
        parse_error(request_api(put, Path400))
    ),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    Path = api_path(["ds", "storages", "messages", "replicas", ThisSite]),
    ?assertMatch(
        {ok, "OK"},
        request_api(put, Path)
    ).

t_leave(_) ->
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    Path = api_path(["ds", "storages", "messages", "replicas", ThisSite]),
    ?assertMatch(
        {error, {_, 400, _}},
        request_api(delete, Path)
    ).

t_leave_notfound(_) ->
    Site = "not_part_of_replica_set",
    Path = api_path(["ds", "storages", "messages", "replicas", Site]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(delete, Path)
    ).

parse_error({ok, Code, JSON}) ->
    {ok, Code, emqx_utils_json:decode(JSON)};
parse_error(Err) ->
    Err.

simple_request(Method, Path, Params) ->
    emqx_mgmt_api_test_util:simple_request(Method, Path, Params).

api_path_cluster(Parts, Config) ->
    [Node1 | _] = ?config(nodes, Config),
    Port = erpc:call(Node1, emqx_config, get, [[dashboard, listeners, http, bind]]),
    Host = "http://127.0.0.1:" ++ integer_to_list(Port),
    emqx_mgmt_api_test_util:api_path(Host, Parts).
