%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_features_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ENV_VAR, "EMQX_FEATURES").
-define(AUTH_HEADER_PD_KEY, {?MODULE, auth_header}).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_config(Key, TCConfig) ->
    case proplists:get_value(Key, TCConfig, undefined) of
        undefined ->
            error({missing_required_config, Key, TCConfig});
        Value ->
            Value
    end.

get_config(Key, TCConfig, Default) ->
    proplists:get_value(Key, TCConfig, Default).

fmt(Fmt, Ctx) ->
    emqx_bridge_v2_testlib:fmt(Fmt, Ctx).

clear_env() ->
    os:unsetenv(?ENV_VAR),
    emqx_machine_features:clear_features(),
    ok.

start_apps(EnvVar, TestCase, TCConfig) ->
    on_exit(fun clear_env/0),
    os:putenv(?ENV_VAR, EnvVar),
    Name0 = fmt(<<"${t}_${n}">>, #{t => TestCase, n => get_counter()}),
    Name = binary_to_list(Name0),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, TCConfig)}
    ),
    on_exit(fun() -> emqx_cth_suite:stop(Apps) end),
    Apps.

get_counter() ->
    Key = {?MODULE, counter},
    case get(Key) of
        undefined ->
            put(Key, 1),
            0;
        N ->
            put(Key, N + 1),
            N
    end.

simple_request(Params) ->
    AuthHeader = get_auth_header(),
    emqx_mgmt_api_test_util:simple_request(Params#{auth_header => AuthHeader}).

get_auth_header() ->
    case get(?AUTH_HEADER_PD_KEY) of
        undefined -> emqx_mgmt_api_test_util:auth_header_();
        Header -> Header
    end.

put_auth_header(Header) ->
    put(?AUTH_HEADER_PD_KEY, Header).

list_features() ->
    URL = emqx_mgmt_api_test_util:api_path(["features"]),
    simple_request(#{
        method => get,
        url => URL
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Smoke tests for listing features via the HTTP API.  Essential preset.
""".
t_list_essential(TCConfig) when is_list(TCConfig) ->
    start_apps("ESSENTIAL", ?FUNCTION_NAME, TCConfig),
    ?assertMatch(
        {200, #{
            <<"preset">> := <<"essential">>,
            <<"enabled">> := [],
            <<"disabled">> := [_ | _]
        }},
        list_features()
    ),
    ok.

-doc """
Smoke tests for listing features via the HTTP API.  Full preset.
""".
t_list_full(TCConfig) when is_list(TCConfig) ->
    start_apps("FULL", ?FUNCTION_NAME, TCConfig),
    ?assertMatch(
        {200, #{
            <<"preset">> := <<"full">>,
            <<"enabled">> := [_ | _],
            <<"disabled">> := []
        }},
        list_features()
    ),
    ok.

-doc """
Smoke tests for listing features via the HTTP API.  Custom preset.
""".
t_list_custom(TCConfig) when is_list(TCConfig) ->
    start_apps("data_integration,dashboard", ?FUNCTION_NAME, TCConfig),
    ?assertMatch(
        {200, #{
            <<"preset">> := <<"custom">>,
            <<"enabled">> := [_ | _],
            <<"disabled">> := [_ | _]
        }},
        list_features()
    ),
    ok.
