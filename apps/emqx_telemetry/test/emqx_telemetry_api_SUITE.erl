%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_telemetry_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/2, request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, #{}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_auth,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_modules,
            emqx_telemetry
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_status_non_official, Config) ->
    meck:new(emqx_telemetry_config, [non_strict, passthrough]),
    meck:expect(emqx_telemetry_config, is_official_version, 0, false),
    %% check non-official telemetry is disable by default
    {ok, _} = emqx:update_config([telemetry], #{}),
    Config;
init_per_testcase(t_status_official, Config) ->
    meck:new(emqx_telemetry_config, [non_strict, passthrough]),
    meck:expect(emqx_telemetry_config, is_official_version, 0, true),
    %% check official telemetry is enable by default
    {ok, _} = emqx:update_config([telemetry], #{}),
    Config;
init_per_testcase(_TestCase, Config) ->
    %% Force enable telemetry to check data.
    {ok, _} = emqx:update_config([telemetry], #{<<"enable">> => true}),
    Config.

end_per_testcase(t_status_non_official, _Config) ->
    meck:unload(emqx_telemetry_config);
end_per_testcase(t_status, _Config) ->
    meck:unload(emqx_telemetry_config);
end_per_testcase(_TestCase, _Config) ->
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

%% official's telemetry is enabled by default
t_status_official(_) ->
    check_status(true).

%% non official's telemetry is disabled by default
t_status_non_official(_) ->
    check_status(false).

check_status(Default) ->
    ct:pal("Check telemetry status:~p~n", [emqx_telemetry_config:is_official_version()]),
    ?assertEqual(Default, is_telemetry_process_enabled()),
    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => (not Default)}
        )
    ),

    {ok, 200, Result0} =
        request(get, uri(["telemetry", "status"])),
    ?assertEqual(
        #{<<"enable">> => (not Default)},
        emqx_utils_json:decode(Result0)
    ),
    ?assertEqual((not Default), is_telemetry_process_enabled()),

    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => (not Default)}
        )
    ),
    ?assertEqual((not Default), is_telemetry_process_enabled()),

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => Default}
        )
    ),

    {ok, 200, Result1} =
        request(get, uri(["telemetry", "status"])),

    ?assertEqual(
        #{<<"enable">> => Default},
        emqx_utils_json:decode(Result1)
    ),
    ?assertEqual(Default, is_telemetry_process_enabled()),

    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => Default}
        )
    ),
    ?assertEqual(Default, is_telemetry_process_enabled()),
    ok.

t_data(_) ->
    ?assert(is_telemetry_process_enabled()),
    {ok, 200, Result} =
        request(get, uri(["telemetry", "data"])),

    ?assertMatch(
        #{
            <<"active_plugins">> := _,
            <<"advanced_mqtt_features">> := _,
            <<"build_info">> := _,
            <<"emqx_version">> := _,
            <<"license">> := _,
            <<"messages_received">> := _,
            <<"mqtt_runtime_insights">> := _,
            <<"nodes_uuid">> := _,
            <<"os_name">> := _,
            <<"otp_version">> := _,
            <<"uuid">> := _,
            <<"vm_specs">> := _
        },
        emqx_utils_json:decode(Result)
    ),

    {ok, 200, _} =
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => false}
        ),

    {ok, 404, _} =
        request(get, uri(["telemetry", "data"])),

    ok.

%% Support emqx:update_config([telemetry], Conf).
t_conf_update(_) ->
    Conf = emqx:get_raw_config([telemetry]),
    ?assert(is_telemetry_process_enabled()),
    {ok, 200, Result1} = request(get, uri(["telemetry", "status"])),
    ?assertEqual(#{<<"enable">> => true}, emqx_utils_json:decode(Result1)),
    {ok, _} = emqx:update_config([telemetry], Conf#{<<"enable">> => false}),
    {ok, 200, Result2} = request(get, uri(["telemetry", "status"])),
    ?assertEqual(#{<<"enable">> => false}, emqx_utils_json:decode(Result2)),
    ?assertNot(is_telemetry_process_enabled()),
    %% reset to true
    {ok, _} = emqx:update_config([telemetry], Conf#{<<"enable">> => true}),
    ?assert(is_telemetry_process_enabled()),
    ok.

is_telemetry_process_enabled() ->
    %% timer is not undefined.
    Timer = element(6, sys:get_state(emqx_telemetry)),
    is_reference(Timer).
