%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request_api/2,
        uri/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_eviction_agent,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    _ = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_status(_Config) ->
    ?assertMatch(
        {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["node_eviction", "status"])
    ),

    ok = emqx_eviction_agent:enable(apitest, undefined),

    ?assertMatch(
        {ok, #{
            <<"status">> := <<"enabled">>,
            <<"stats">> := #{}
        }},
        api_get(["node_eviction", "status"])
    ),

    ok = emqx_eviction_agent:disable(apitest),

    ?assertMatch(
        {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["node_eviction", "status"])
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    case request_api(get, uri(Path)) of
        {ok, ResponseBody} ->
            {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
        {error, _} = Error ->
            Error
    end.
