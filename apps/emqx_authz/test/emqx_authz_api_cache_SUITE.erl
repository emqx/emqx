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

-module(emqx_authz_api_cache_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_api_test_helpers, [request/2, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz, emqx_dashboard, emqx_management],
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
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authz, emqx_conf, emqx_management]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], true),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

t_clean_cahce(_) ->
    {ok, C} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"a/b/c">>, 0),
    ok = emqtt:publish(C, <<"a/b/c">>, <<"{\"x\":1,\"y\":1}">>, 0),

    {ok, 200, Result3} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(2, length(jsx:decode(Result3))),

    request(delete, uri(["authorization", "cache"])),

    {ok, 200, Result4} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(0, length(jsx:decode(Result4))),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
