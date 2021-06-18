%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_ct_helpers:start_apps([emqx_authz], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_authz]).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    ok;

set_special_configs(_App) ->
    ok.

-define(RULE1, [<<"test/%u">>,<<"pub">>]).
-define(RULE2, [<<"test/%c">>,<<"pub">>]).
-define(RULE3, [<<"#">>,<<"sub">>]).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo = #{clientid => <<"clientid">>,
                    username => <<"username">>
                   },

    ?assertEqual(nomatch,
        emqx_authz_redis:do_check_authz(
            #{}, subscribe, <<"#">>, []
         )),
    ?assertEqual(nomatch,
        emqx_authz_redis:do_check_authz(
          ClientInfo, subscribe, <<"test/clientid">>, ?RULE1 ++ ?RULE2
         )),
    ?assertEqual({matched, allow},
        emqx_authz_redis:do_check_authz(
          ClientInfo, publish, <<"test/clientid">>, ?RULE1 ++ ?RULE2
         )),
    ?assertEqual(nomatch,
        emqx_authz_redis:do_check_authz(
          ClientInfo, subscribe, <<"test/username">>, ?RULE1 ++ ?RULE2
         )),
    ?assertEqual({matched, allow},
        emqx_authz_redis:do_check_authz(
          ClientInfo, publish, <<"test/clientid">>, ?RULE1 ++ ?RULE2
         )),
    ?assertEqual({matched, allow},
        emqx_authz_redis:do_check_authz(
          ClientInfo, subscribe, <<"#">>, ?RULE3
         )),
    ?assertEqual(nomatch,
        emqx_authz_redis:do_check_authz(
          ClientInfo, publish, <<"#">>, ?RULE3
         )),
    ok.

