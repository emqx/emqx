%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_mgmt_api_test_util, [request/2, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_auth, #{
                config =>
                    #{
                        authorization =>
                            #{
                                cache => #{enabled => true},
                                no_match => deny,
                                sources => []
                            }
                    }
            }},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_clean_cache(_) ->
    {ok, C} = emqtt:start_link([{clientid, <<"emqx0">>}, {username, <<"emqx0">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"a/b/c">>, 0),
    ok = emqtt:publish(C, <<"a/b/c">>, <<"{\"x\":1,\"y\":1}">>, 0),

    {ok, 200, Result3} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(2, length(emqx_utils_json:decode(Result3))),

    request(delete, uri(["authorization", "cache"])),

    {ok, 200, Result4} = request(get, uri(["clients", "emqx0", "authorization", "cache"])),
    ?assertEqual(0, length(emqx_utils_json:decode(Result4))),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
