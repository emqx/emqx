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

-module(emqx_authz_file_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
           [emqx_conf, emqx_authz],
           fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();

set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_ok(_Config) ->
    ClientInfo = #{clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    ok = setup_rules([{allow, {user, "username"}, publish, ["t"]}]),
    ok = setup_config(#{}),

    ?assertEqual(
       allow,
       emqx_access_control:authorize(ClientInfo, publish, <<"t">>)),

    ?assertEqual(
       deny,
       emqx_access_control:authorize(ClientInfo, subscribe, <<"t">>)).

t_invalid_file(_Config) ->
    ok = file:write_file(<<"acl.conf">>, <<"{{invalid term">>),

    ?assertMatch(
       {error, bad_acl_file_content},
       emqx_authz:update(?CMD_REPLACE, [raw_file_authz_config()])).

t_nonexistent_file(_Config) ->
    ?assertEqual(
       {error, failed_to_read_acl_file},
       emqx_authz:update(?CMD_REPLACE,
                         [maps:merge(raw_file_authz_config(),
                                     #{<<"path">> => <<"nonexistent.conf">>})
                         ])).

t_update(_Config) ->
    ok = setup_rules([{allow, {user, "username"}, publish, ["t"]}]),
    ok = setup_config(#{}),

    ?assertMatch(
       {error, _},
       emqx_authz:update(
         {?CMD_REPLACE, file},
         maps:merge(raw_file_authz_config(),
                    #{<<"path">> => <<"nonexistent.conf">>}))),

    ?assertMatch(
       {ok, _},
       emqx_authz:update(
         {?CMD_REPLACE, file},
         raw_file_authz_config())).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_file_authz_config() ->
    #{
        <<"enable">> => <<"true">>,

        <<"type">> => <<"file">>,
        <<"path">> => <<"acl.conf">>
    }.

setup_rules(Rules) ->
    {ok, F} = file:open(<<"acl.conf">>, [write]),
    lists:foreach(
      fun(Rule) ->
              io:format(F, "~p.~n", [Rule])
      end,
      Rules),
    ok = file:close(F).

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
      raw_file_authz_config(),
      SpecialParams).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
