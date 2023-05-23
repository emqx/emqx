%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_mgmt_api_test_util:init_suite([emqx_eviction_agent]),
    Config.

end_per_suite(Config) ->
    emqx_mgmt_api_test_util:end_suite([emqx_eviction_agent]),
    Config.

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
