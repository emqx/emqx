%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_cluster_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(SOURCE_HTTP, #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://fake.com:443/acl?username=", ?PH_USERNAME/binary>>,
    <<"ssl">> => #{<<"enable">> => true},
    <<"headers">> => #{},
    <<"method">> => <<"get">>,
    <<"request_timeout">> => <<"5s">>
}).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Cluster = mk_cluster_spec(#{}),
    Nodes = [NodePrimary | _] = emqx_cth_cluster:start(Cluster, #{work_dir => WorkDir}),
    lists:foreach(fun(N) -> ?ON(N, emqx_authz_test_lib:register_fake_sources([http])) end, Nodes),
    {ok, App} = ?ON(NodePrimary, emqx_common_test_http:create_default_app()),
    [{api, App}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config)).

t_api(Config) ->
    APINode = ?config(node, Config),
    {ok, 200, Result1} = request(get, uri(["authorization", "sources"]), [], Config),
    ?assertEqual([], get_sources(Result1)),
    {ok, 204, _} = request(post, uri(["authorization", "sources"]), ?SOURCE_HTTP, Config),
    AllNodes = ?config(cluster_nodes, Config),
    [OtherNode] = AllNodes -- [APINode],
    lists:foreach(
        fun(N) ->
            ?ON(
                N,
                ?assertMatch(
                    [#{<<"type">> := <<"http">>}],
                    emqx:get_raw_config([authorization, sources])
                )
            )
        end,
        AllNodes
    ),
    %% delete the source from the second node.
    ?ON(OtherNode, begin
        {ok, _} = emqx:update_config([authorization], #{<<"sources">> => []}),
        ?assertMatch([], emqx:get_raw_config([authorization, sources]))
    end),
    ?assertMatch(
        {ok, 204, _},
        request(
            delete,
            uri(["authorization", "sources", "http"]),
            [],
            Config
        )
    ),
    lists:foreach(
        fun(N) ->
            ?ON(
                N,
                ?assertMatch(
                    [],
                    emqx:get_raw_config([authorization, sources])
                )
            )
        end,
        AllNodes
    ),
    ?assertMatch(
        {ok, 404, _},
        request(
            delete,
            uri(["authorization", "sources", "http"]),
            [],
            Config
        )
    ),
    ok.

get_sources(Result) ->
    maps:get(<<"sources">>, emqx_utils_json:decode(Result)).

mk_cluster_spec(Opts) ->
    Apps = [
        emqx,
        {emqx_conf, "authorization {cache{enable=false},no_match=deny,sources=[]}"},
        emqx_auth,
        emqx_management
    ],
    Node1Apps = Apps ++ [{emqx_dashboard, "dashboard.listeners.http {enable=true,bind=18083}"}],
    Node2Apps = Apps,
    [
        {emqx_authz_api_cluster_SUITE1, Opts#{apps => Node1Apps}},
        {emqx_authz_api_cluster_SUITE2, Opts#{apps => Node2Apps}}
    ].

request(Method, URL, Body, Config) ->
    AuthHeader = emqx_common_test_http:auth_header(?config(api, Config)),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], AuthHeader, Body, Opts).
