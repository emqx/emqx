%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(ON(NODE, BODY),
    emqx_ds_test_helpers:on(NODE, fun() -> BODY end)
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    AppSpecs = [
        emqx_conf,
        {emqx, "durable_sessions.enable = true"},
        emqx_management
    ],
    DashboardSpecs = [
        emqx_mgmt_api_test_util:emqx_dashboard()
    ],
    Cluster = emqx_cth_cluster:start(
        [
            {emqx_mgmt_api_ds_SUITE1, #{apps => AppSpecs ++ DashboardSpecs}},
            {emqx_mgmt_api_ds_SUITE2, #{apps => AppSpecs}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [N1 | _] = Cluster,
    Sites = [?ON(N, emqx_ds_builtin_raft_meta:this_site()) || N <- Cluster],
    ApiAuth = ?ON(N1, emqx_common_test_http:default_auth_header()),
    [{cluster, Cluster}, {sites, Sites}, {api_auth, ApiAuth} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

t_get_sites(Config) ->
    {ok, Response} = request_api(get, ["ds", "sites"], Config),
    ?assertSameSet(
        ?config(sites, Config),
        emqx_utils_json:decode(Response)
    ).

t_get_storages(Config) ->
    {ok, Response} = request_api(get, ["ds", "storages"], Config),
    ?assertEqual(
        [<<"messages">>],
        emqx_utils_json:decode(Response)
    ).

t_get_site(Config) ->
    %% Unknown sites must result in error 404:
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, ["ds", "sites", "unknown_site"], Config)
    ),
    %% Valid request:
    [N1 | _] = ?config(cluster, Config),
    [S1 | _] = ?config(sites, Config),
    N1Bin = atom_to_binary(N1),
    {ok, Resp1} = request_api(get, ["ds", "sites", S1], Config),
    ?assertMatch(
        #{
            <<"node">> := N1Bin,
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
        emqx_utils_json:decode(Resp1)
    ),
    %% Transitions are reported:
    ?assertMatch(
        {ok, "OK"},
        request_api(delete, ["ds", "storages", "messages", "replicas", S1], Config)
    ),
    {ok, Resp2} = request_api(get, ["ds", "sites", S1], Config),
    ?assertMatch(
        #{
            <<"shards">> :=
                [
                    #{
                        <<"storage">> := <<"messages">>,
                        <<"id">> := _,
                        <<"status">> := <<"up">>,
                        <<"transition">> := <<"leaving">>
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Resp2)
    ),
    %% Wait until transitions are complete:
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(messages)),
    %% Restore initial allocation:
    ?assertMatch(
        {ok, "OK"},
        request_api(put, ["ds", "storages", "messages", "replicas", S1], Config)
    ),
    {ok, Resp3} = request_api(get, ["ds", "sites", S1], Config),
    ?assertMatch(
        #{
            <<"shards">> :=
                [
                    #{
                        <<"storage">> := <<"messages">>,
                        <<"id">> := _,
                        <<"status">> := <<"up">>,
                        <<"transition">> := <<"joining">>
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Resp3)
    ),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(messages)).

t_get_db(Config) ->
    [N1 | _] = ?config(cluster, Config),
    [S1, S2] = lists:sort(?config(sites, Config)),
    %% Unknown DBs must result in error 404:
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, ["ds", "storages", "unknown_ds"], Config)
    ),
    %% Valid request:
    {ok, Resp1} = request_api(get, ["ds", "storages", "messages"], Config),
    ?assertMatch(
        #{
            <<"name">> := <<"messages">>,
            <<"shards">> :=
                [
                    #{
                        <<"id">> := _,
                        <<"replicas">> :=
                            [
                                #{<<"site">> := S1, <<"status">> := <<"up">>},
                                #{<<"site">> := S2, <<"status">> := <<"up">>}
                            ]
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Resp1)
    ),
    %% Transitions are reported:
    ?assertMatch(
        {ok, "OK"},
        request_api(delete, ["ds", "storages", "messages", "replicas", S1], Config)
    ),
    {ok, Resp2} = request_api(get, ["ds", "storages", "messages"], Config),
    ?assertMatch(
        #{
            <<"shards">> :=
                [
                    #{
                        <<"transitions">> :=
                            [#{<<"site">> := S1, <<"transition">> := <<"leaving">>}]
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Resp2)
    ),
    %% Wait until transitions are complete:
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(messages)),
    %% Restore initial allocation:
    ?assertMatch(
        {ok, "OK"},
        request_api(put, ["ds", "storages", "messages", "replicas", S1], Config)
    ),
    {ok, Resp3} = request_api(get, ["ds", "storages", "messages"], Config),
    ?assertMatch(
        #{
            <<"shards">> :=
                [
                    #{
                        <<"transitions">> :=
                            [#{<<"site">> := S1, <<"transition">> := <<"joining">>}]
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Resp3)
    ),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(messages)).

t_get_replicas(Config) ->
    %% Unknown DBs must result in error 404:
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, ["ds", "storages", "unknown_ds", "replicas"], Config)
    ),
    %% Valid path:
    {ok, Response} = request_api(get, ["ds", "storages", "messages", "replicas"], Config),
    Sites = ?config(sites, Config),
    ?assertSameSet(
        Sites,
        emqx_utils_json:decode(Response)
    ).

t_put_replicas(Config) ->
    %% Error cases:
    ?assertMatch(
        {ok, 400, #{<<"message">> := <<"Unknown sites: invalid_site">>}},
        parse_error(
            request_api(
                put,
                ["ds", "storages", "messages", "replicas"],
                [<<"invalid_site">>],
                Config
            )
        )
    ),
    %% Success case:
    [S1 | _] = ?config(sites, Config),
    ?assertMatch(
        {ok, 202, <<"OK">>},
        request_api(put, ["ds", "storages", "messages", "replicas"], [S1], Config)
    ).

t_join(Config) ->
    ReplicasApiPath = ["ds", "storages", "messages", "replicas"],
    ?assertMatch(
        {ok, 400, #{<<"message">> := <<"Unknown sites: unknown_site">>}},
        parse_error(request_api(put, ReplicasApiPath ++ ["unknown_site"], <<>>, Config))
    ),
    [S1 | _] = ?config(sites, Config),
    ?assertMatch(
        {ok, "OK"},
        request_api(put, ReplicasApiPath ++ [S1], Config)
    ).

t_leave(Config) ->
    ReplicasApiPath = ["ds", "storages", "messages", "replicas"],
    [S1, S2] = ?config(sites, Config),
    ?assertMatch(
        {ok, "OK"},
        request_api(delete, ReplicasApiPath ++ [S1], Config)
    ),
    %% Leaving the last site is prohibited:
    ?assertMatch(
        {ok, 400, #{<<"message">> := <<"Replica sets would become too small">>}},
        parse_error(request_api(delete, ReplicasApiPath ++ [S2], <<>>, Config))
    ),
    %% Restore initial allocation:
    ?assertMatch(
        {ok, "OK"},
        request_api(put, ReplicasApiPath ++ [S1], Config)
    ),
    [N1 | _] = ?config(cluster, Config),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(messages)).

t_leave_notfound(Config) ->
    Site = "not_part_of_replica_set",
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(delete, ["ds", "storages", "messages", "replicas", Site], Config)
    ).

%%

request_api(Method, ApiPath, Config) ->
    Url = emqx_mgmt_api_test_util:api_path(ApiPath),
    emqx_mgmt_api_test_util:request_api(Method, Url, ?config(api_auth, Config)).

request_api(Method, ApiPath, Body, Config) ->
    Url = emqx_mgmt_api_test_util:api_path(ApiPath),
    emqx_mgmt_api_test_util:request_api_with_body(Method, Url, ?config(api_auth, Config), Body).

parse_error({ok, Code, JSON}) ->
    {ok, Code, emqx_utils_json:decode(JSON)};
parse_error(Err) ->
    Err.
