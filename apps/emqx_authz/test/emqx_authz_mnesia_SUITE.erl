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

-module(emqx_authz_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
           [emqx_conf, emqx_authz],
           fun set_special_configs/1
          ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
                [authorization],
                #{<<"no_match">> => <<"allow">>,
                  <<"cache">> => #{<<"enable">> => <<"true">>},
                  <<"sources">> => []}),
    emqx_common_test_helpers:stop_apps([emqx_authz, emqx_conf]),
    ok.

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources],
                                 [#{<<"type">> => <<"built-in-database">>}]),
    ok;
set_special_configs(_App) ->
    ok.

init_per_testcase(t_authz, Config) ->
    mria:dirty_write(#emqx_acl{who = {?ACL_TABLE_USERNAME, <<"test_username">>},
                               rules = [{allow, publish, <<"test/", ?PH_S_USERNAME>>},
                                        {allow, subscribe, <<"eq #">>}
                                       ]
                              }),
    mria:dirty_write(#emqx_acl{who = {?ACL_TABLE_CLIENTID, <<"test_clientid">>},
                               rules = [{allow, publish, <<"test/", ?PH_S_CLIENTID>>},
                                        {deny, subscribe, <<"eq #">>}
                                       ]
                              }),
    mria:dirty_write(#emqx_acl{who = ?ACL_TABLE_ALL,
                               rules = [{deny, all, <<"#">>}]
                              }),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(t_authz, Config) ->
    [ mria:dirty_delete(?ACL_TABLE, K) || K <- mnesia:dirty_all_keys(?ACL_TABLE)],
    Config;
end_per_testcase(_, Config) -> Config.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    listener => {tcp, default}
                   },
    ClientInfo2 = #{clientid => <<"fake_clientid">>,
                    username => <<"test_username">>,
                    peerhost => {127,0,0,1},
                    listener => {tcp, default}
                   },
    ClientInfo3 = #{clientid => <<"test_clientid">>,
                    username => <<"fake_username">>,
                    peerhost => {127,0,0,1},
                    listener => {tcp, default}
                   },

    ?assertEqual(deny, emqx_access_control:authorize(
                         ClientInfo1, subscribe, <<"#">>)),
    ?assertEqual(deny, emqx_access_control:authorize(
                         ClientInfo1, publish, <<"#">>)),

    ?assertEqual(allow, emqx_access_control:authorize(
                          ClientInfo2, publish, <<"test/test_username">>)),
    ?assertEqual(allow, emqx_access_control:authorize(
                          ClientInfo2, subscribe, <<"#">>)),

    ?assertEqual(allow, emqx_access_control:authorize(
                          ClientInfo3, publish, <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(
                          ClientInfo3, subscribe, <<"#">>)),

    ok.
