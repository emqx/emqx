%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_dashboard_api_test_helpers, [request/2, request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, #{}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),

    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authn, emqx_authz, emqx_modules, emqx_dashboard],
        fun set_special_configs/1
    ),

    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([
        emqx_dashboard, emqx_conf, emqx_authn, emqx_authz, emqx_modules
    ]),
    ok.

init_per_testcase(t_status_non_official, Config) ->
    meck:new(emqx_telemetry, [non_strict, passthrough]),
    meck:expect(emqx_telemetry, official_version, 1, false),
    Config;
init_per_testcase(t_status, Config) ->
    meck:new(emqx_telemetry, [non_strict, passthrough]),
    meck:expect(emqx_telemetry, enable, fun() -> ok end),
    {ok, _, _} =
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => true}
        ),
    Config;
init_per_testcase(_TestCase, Config) ->
    {ok, _, _} =
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => true}
        ),
    Config.

end_per_testcase(t_status_non_official, _Config) ->
    meck:unload(emqx_telemetry);
end_per_testcase(t_status, _Config) ->
    meck:unload(emqx_telemetry);
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

t_status(_) ->
    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => false}
        )
    ),

    {ok, 200, Result0} =
        request(get, uri(["telemetry", "status"])),

    ?assertEqual(
        #{<<"enable">> => false},
        jsx:decode(Result0)
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => false}
        )
    ),

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => true}
        )
    ),

    {ok, 200, Result1} =
        request(get, uri(["telemetry", "status"])),

    ?assertEqual(
        #{<<"enable">> => true},
        jsx:decode(Result1)
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => true}
        )
    ).

t_status_non_official(_) ->
    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            uri(["telemetry", "status"]),
            #{<<"enable">> => false}
        )
    ).

t_data(_) ->
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
        jsx:decode(Result)
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
