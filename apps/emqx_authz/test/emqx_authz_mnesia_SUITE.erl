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

-define(CONF_DEFAULT, <<"authorization: {sources: []}">>).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, fields, fun("authorization") ->
                                             meck:passthrough(["authorization"]) ++
                                             emqx_authz_schema:fields("authorization");
                                        (F) -> meck:passthrough([F])
                                     end),

    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),
    ok = emqx_ct_helpers:start_apps([emqx_authz]),

    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    Rules = [#{<<"type">> => <<"built-in-database">>}],
    {ok, _} = emqx_authz:update(replace, Rules),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    emqx_ct_helpers:stop_apps([emqx_authz]),
    meck:unload(emqx_schema),
    ok.

init_per_testcase(t_authz, Config) ->
    mnesia:transaction(fun mnesia:write/1, [#emqx_acl{who = {username, <<"test_username">>},
                                                      rules = [{allow, publish, <<"test/%u">>},
                                                               {allow, subscribe, <<"eq #">>}
                                                              ]
                                                     }]),
    mnesia:transaction(fun mnesia:write/1, [#emqx_acl{who = {clientid, <<"test_clientid">>},
                                                      rules = [{allow, publish, <<"test/%c">>},
                                                               {deny, subscribe, <<"eq #">>}
                                                              ]
                                                     }]),
    mnesia:transaction(fun mnesia:write/1, [#emqx_acl{who = all,
                                                      rules = [{deny, all, <<"#">>}]
                                                     }]),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(t_authz, Config) ->
    [ mnesia:dirty_delete(?ACL_TABLE, K) || K <- mnesia:dirty_all_keys(?ACL_TABLE)],
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

    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"#">>)), 
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, publish, <<"#">>)),

    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo2, publish, <<"test/test_username">>)),
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo2, subscribe, <<"#">>)),

    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo3, publish, <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(ClientInfo3, subscribe, <<"#">>)),

    ok.

