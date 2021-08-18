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

-define(CONF_DEFAULT, <<"authorization: {rules: []}">>).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),
    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    {ok, _} = emqx:update_config([zones, default, authorization, cache, enable], false),
    {ok, _} = emqx:update_config([zones, default, authorization, enable], true),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    emqx_ct_helpers:stop_apps([emqx_authz]),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    Config.

-define(RULE1, #{<<"principal">> => <<"all">>,
                 <<"topics">> => [<<"#">>],
                 <<"action">> => <<"all">>,
                 <<"permission">> => <<"deny">>}
       ).
-define(RULE2, #{<<"principal">> =>
                    #{<<"ipaddress">> => <<"127.0.0.1">>},
                 <<"topics">> =>
                        [#{<<"eq">> => <<"#">>},
                         #{<<"eq">> => <<"+">>}
                        ] ,
                 <<"action">> => <<"all">>,
                 <<"permission">> => <<"allow">>}
       ).
-define(RULE3,#{<<"principal">> =>
                    #{<<"and">> => [#{<<"username">> => <<"^test?">>},
                                    #{<<"clientid">> => <<"^test?">>}
                                   ]},
                <<"topics">> => [<<"test">>],
                <<"action">> => <<"publish">>,
                <<"permission">> => <<"allow">>}
       ).
-define(RULE4,#{<<"principal">> =>
                    #{<<"or">> => [#{<<"username">> => <<"^test">>},
                                   #{<<"clientid">> => <<"test?">>}
                                  ]},
                <<"topics">> => [<<"%u">>,<<"%c">>],
                <<"action">> => <<"publish">>,
                <<"permission">> => <<"deny">>}
       ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_update_rule(_) ->
    {ok, _} = emqx_authz:update(replace, [?RULE2]),
    {ok, _} = emqx_authz:update(head, [?RULE1]),
    {ok, _} = emqx_authz:update(tail, [?RULE3]),

    Lists1 = emqx_authz:check_rules([?RULE1, ?RULE2, ?RULE3]),
    ?assertMatch(Lists1, emqx:get_config([authorization, rules], [])),

    [#{annotations := #{id := Id1,
                        principal := all,
                        topics := [['#']]}
      },
     #{annotations := #{id := Id2,
                        principal := #{ipaddress := {{127,0,0,1},{127,0,0,1},32}},
                        topics := [#{eq := ['#']}, #{eq := ['+']}]}
      },
     #{annotations := #{id := Id3,
                        principal :=
                             #{'and' := [#{username := {re_pattern, _, _, _, _}},
                                         #{clientid := {re_pattern, _, _, _, _}}
                                        ]
                              },
                        topics := [[<<"test">>]]}
      }
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:update({replace_once, Id3}, ?RULE4),
    Lists2 = emqx_authz:check_rules([?RULE1, ?RULE2, ?RULE4]),
    ?assertMatch(Lists2, emqx:get_config([authorization, rules], [])),

    [#{annotations := #{id := Id1,
                        principal := all,
                        topics := [['#']]}
      },
     #{annotations := #{id := Id2,
                        principal := #{ipaddress := {{127,0,0,1},{127,0,0,1},32}},
                        topics := [#{eq := ['#']},
                                   #{eq := ['+']}]}
      },
     #{annotations := #{id := Id3,
                        principal :=
                              #{'or' := [#{username := {re_pattern, _, _, _, _}},
                                         #{clientid := {re_pattern, _, _, _, _}}
                                        ]
                               },
                        topics := [#{pattern := [<<"%u">>]},
                                   #{pattern := [<<"%c">>]}
                                  ]}
      }
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:update(replace, []).

t_move_rule(_) ->
    {ok, _} = emqx_authz:update(replace, [?RULE1, ?RULE2, ?RULE3, ?RULE4]),
    [#{annotations := #{id := Id1}},
     #{annotations := #{id := Id2}},
     #{annotations := #{id := Id3}},
     #{annotations := #{id := Id4}}
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:move(Id4, <<"top">>),
    ?assertMatch([#{annotations := #{id := Id4}},
                  #{annotations := #{id := Id1}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id3}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id1, <<"bottom">>),
    ?assertMatch([#{annotations := #{id := Id4}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id3}},
                  #{annotations := #{id := Id1}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id3, #{<<"before">> => Id4}),
    ?assertMatch([#{annotations := #{id := Id3}},
                  #{annotations := #{id := Id4}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id1}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id2, #{<<"after">> => Id1}),
    ?assertMatch([#{annotations := #{id := Id3}},
                  #{annotations := #{id := Id4}},
                  #{annotations := #{id := Id1}},
                  #{annotations := #{id := Id2}}
                 ], emqx_authz:lookup()),
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

    Rules1 = [emqx_authz:init_rule(Rule) || Rule <- emqx_authz:check_rules([?RULE1, ?RULE2])],
    Rules2 = [emqx_authz:init_rule(Rule) || Rule <- emqx_authz:check_rules([?RULE2, ?RULE1])],
    Rules3 = [emqx_authz:init_rule(Rule) || Rule <- emqx_authz:check_rules([?RULE3, ?RULE4])],
    Rules4 = [emqx_authz:init_rule(Rule) || Rule <- emqx_authz:check_rules([?RULE4, ?RULE1])],

    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"#">>, deny, [])),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"+">>, deny, Rules1)),
    ?assertEqual({stop, allow},
        emqx_authz:authorize(ClientInfo1, subscribe, <<"+">>, deny, Rules2)),
    ?assertEqual({stop, allow},
        emqx_authz:authorize(ClientInfo1, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo1, publish, <<"test">>, deny, Rules4)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo2, subscribe, <<"#">>, deny, Rules2)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo3, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo3, publish, <<"fake">>, deny, Rules4)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo4, publish, <<"test">>, deny, Rules3)),
    ?assertEqual({stop, deny},
        emqx_authz:authorize(ClientInfo4, publish, <<"fake">>, deny, Rules4)),
    ok.
