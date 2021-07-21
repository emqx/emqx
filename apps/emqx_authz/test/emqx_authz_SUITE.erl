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

-module(emqx_authz_SUITE).

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
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, includes, fun() -> ["emqx_authz"] end ),

    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    ok = emqx_config:update_config([zones, default, acl, cache, enable], false),
    ok = emqx_config:update_config([emqx_authz, enable], true),
    ok = emqx_config:update_config([emqx_authz, deny_action], reply),
    emqx_authz:update(replace, []),
    Config.

end_per_suite(_Config) ->
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'authz.conf')),
    emqx_ct_helpers:stop_apps([emqx_authz]).

-define(RULE1, #{principal => all,
                 topics => [<<"#">>],
                 action => all,
                 permission => deny}
       ).
-define(RULE2, #{principal =>
                    #{ipaddress => <<"127.0.0.1">>},
                 topics =>
                        [#{eq => <<"#">>},
                         #{eq => <<"+">>}
                        ] ,
                 action => all,
                 permission => allow}
       ).
-define(RULE3,#{principal =>
                    #{'and' => [#{username => "^test?"},
                                    #{clientid => "^test?"}
                                   ]},
                topics => [<<"test">>],
                action => publish,
                permission => allow}
       ).
-define(RULE4,#{principal =>
                    #{'or' => [#{username => <<"^test">>},
                               #{clientid => <<"test?">>}
                              ]},
                topics => [<<"%u">>,<<"%c">>],
                action => publish,
                permission => deny}
       ).


%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------
t_init_rule(_) ->
    ?assertEqual(#{permission => deny,
                   action => all,
                   principal => all,
                   topics => [['#']]
                  }, emqx_authz:init_rule(?RULE1)),
    ?assertEqual(#{permission => allow,
                   action => all,
                   principal =>
                        #{ipaddress => {{127,0,0,1},{127,0,0,1},32}},
                   topics => [#{eq => ['#']},
                              #{eq => ['+']}]
                  }, emqx_authz:init_rule(?RULE2)),
    ?assertMatch(
       #{permission := allow,
         action := publish,
         principal :=
                #{'and' := [#{username := {re_pattern, _, _, _, _}},
                            #{clientid := {re_pattern, _, _, _, _}}
                           ]
                 },
         topics := [[<<"test">>]]
        }, emqx_authz:init_rule(?RULE3)),
    ?assertMatch(
       #{permission := deny,
         action := publish,
         principal :=
                #{'or' := [#{username := {re_pattern, _, _, _, _}},
                           #{clientid := {re_pattern, _, _, _, _}}
                          ]
                 },
         topics := [#{pattern := [<<"%u">>]},
                    #{pattern := [<<"%c">>]}
                   ]
        }, emqx_authz:init_rule(?RULE4)),
    ok.

t_authz(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo2 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {192,168,0,10},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo3 = #{clientid => <<"test">>,
                    username => <<"fake">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo4 = #{clientid => <<"fake">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },

    Rules1 = [emqx_authz:init_rule(Rule) || Rule <- [?RULE1, ?RULE2]],
    Rules2 = [emqx_authz:init_rule(Rule) || Rule <- [?RULE2, ?RULE1]],
    Rules3 = [emqx_authz:init_rule(Rule) || Rule <- [?RULE3, ?RULE4]],
    Rules4 = [emqx_authz:init_rule(Rule) || Rule <- [?RULE4, ?RULE1]],

    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"#">>, deny, [])),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"+">>, deny, Rules1)),
    ?assertEqual({stop, allow},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"+">>, deny, Rules2)),
    ?assertEqual({stop, allow},
        emqx_authz:authorize(ClientInfo1, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo1, publish, <<"test">>, deny, Rules4)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo2, subscribe, <<"#">>, deny, Rules2)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo3, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo3, publish, <<"fake">>, deny, Rules4)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo4, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, {deny, reply}},
        emqx_authz:authorize(ClientInfo4, publish, <<"fake">>, deny, Rules4)),
    ok.
