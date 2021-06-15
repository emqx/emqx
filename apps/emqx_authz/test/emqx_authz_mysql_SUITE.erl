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

-module(emqx_authz_mysql_SUITE).

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

-define(RULE1, [[0,null,<<"$all">>,null,3,<<"#">>]]).
-define(RULE2, [[1,<<"127.0.0.1">>,null,null,3,<<"eq #">>]]).
-define(RULE3, [[1,null,<<"^test?">>,<<"^test?">>,2,<<"test">>]]).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1}
                   },
    ClientInfo2 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {192,168,0,10}
                   },
    ClientInfo3 = #{clientid => <<"test">>,
                    username => <<"fake">>
                   },

    ?assertEqual(nomatch,
        emqx_authz_mysql:do_check_authz(
            #{}, subscribe, <<"#">>, []
         )),
    ?assertEqual({matched, deny},
        emqx_authz_mysql:do_check_authz(
          ClientInfo1, subscribe, <<"+">>, ?RULE1 ++ ?RULE2
         )),
    ?assertEqual({matched, allow},
        emqx_authz_mysql:do_check_authz(
          ClientInfo1, subscribe, <<"#">>, ?RULE2 ++ ?RULE1
         )),
    ?assertEqual({matched, deny},
        emqx_authz_mysql:do_check_authz(
          ClientInfo2, subscribe, <<"#">>, ?RULE2 ++ ?RULE1
         )),
    ?assertEqual({matched, allow},
        emqx_authz_mysql:do_check_authz(
          ClientInfo2, publish, <<"test">>, ?RULE3
         )),
    ?assertEqual(nomatch,
        emqx_authz_mysql:do_check_authz(
          ClientInfo3, publish, <<"test">>, ?RULE3
         )),
    ok.

