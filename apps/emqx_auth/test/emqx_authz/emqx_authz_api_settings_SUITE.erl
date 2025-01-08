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

-module(emqx_authz_api_settings_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization =>
                        #{
                            cache => #{enable => true},
                            no_match => allow,
                            sources => []
                        }
                }
            }},
            emqx_auth,
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
            <<"ttl">> => <<"60s">>,
            <<"excludes">> => [<<"nocache/#">>]
        }
    },

    {ok, 200, Result1} = request(put, uri(["authorization", "settings"]), Settings1),
    {ok, 200, Result1} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings1, emqx_utils_json:decode(Result1)),

    Settings2 = #{
        <<"no_match">> => <<"allow">>,
        <<"deny_action">> => <<"ignore">>,
        <<"cache">> => #{
            <<"enable">> => true,
            <<"max_size">> => 32,
            <<"ttl">> => <<"60s">>
        }
    },

    {ok, 200, Result2} = request(put, uri(["authorization", "settings"]), Settings2),
    {ok, 200, Result2} = request(get, uri(["authorization", "settings"]), []),
    Cache = maps:get(<<"cache">>, Settings2),
    ExpectedSettings2 = Settings2#{<<"cache">> => Cache#{<<"excludes">> => []}},
    ?assertEqual(ExpectedSettings2, emqx_utils_json:decode(Result2)),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
