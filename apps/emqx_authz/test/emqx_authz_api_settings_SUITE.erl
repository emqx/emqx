%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_settings_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_api_test_helpers, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz, emqx_dashboard],
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
    ok = stop_apps([emqx_resource, emqx_connector]),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authz, emqx_conf]),
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
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    Settings1 = #{
        <<"no_match">> => <<"deny">>,
        <<"deny_action">> => <<"disconnect">>,
        <<"cache">> => #{
            <<"enable">> => false,
            <<"max_size">> => 32,
            <<"ttl">> => 60000
        }
    },

    {ok, 200, Result1} = request(put, uri(["authorization", "settings"]), Settings1),
    {ok, 200, Result1} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings1, jsx:decode(Result1)),

    Settings2 = #{
        <<"no_match">> => <<"allow">>,
        <<"deny_action">> => <<"ignore">>,
        <<"cache">> => #{
            <<"enable">> => true,
            <<"max_size">> => 32,
            <<"ttl">> => 60000
        }
    },

    {ok, 200, Result2} = request(put, uri(["authorization", "settings"]), Settings2),
    {ok, 200, Result2} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings2, jsx:decode(Result2)),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
