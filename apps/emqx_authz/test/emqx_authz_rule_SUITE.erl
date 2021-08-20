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

-module(emqx_authz_rule_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(RULE1, {deny,  all, all, ["#"]}).
-define(RULE2, {allow, {ipaddr,  "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]}).
-define(RULE3, {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, ["%c"]}).
-define(RULE4, {allow, {'and', [{clientid, "^test?"}, {username, "^test?"}]}, publish, ["topic/test"]}).
-define(RULE5, {allow, {'or',  [{username, "^test"},  {clientid, "test?"}]},  publish, ["%u", "%c"]}).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_authz]),
    ok.

t_compile(_) ->
    ?assertEqual({deny, all, all, [['#']]}, emqx_authz_rule:compile(?RULE1)),

    ?assertEqual({allow, {ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}}, all, [{eq, ['#']}, {eq, ['+']}]}, emqx_authz_rule:compile(?RULE2)),

    ?assertEqual({allow,
                  {ipaddrs,[{{127,0,0,1},{127,0,0,1},32},
                            {{192,168,1,0},{192,168,1,255},24}]},
                  subscribe,
                  [{pattern,[<<"%c">>]}]
               }, emqx_authz_rule:compile(?RULE3)),

    ?assertMatch({allow,
                  {'and', [{clientid, {re_pattern, _, _, _, _}}, {username, {re_pattern, _, _, _, _}}]},
                  publish,
                  [[<<"topic">>, <<"test">>]]
                 }, emqx_authz_rule:compile(?RULE4)),

    ?assertMatch({allow,
                  {'or', [{username, {re_pattern, _, _, _, _}}, {clientid, {re_pattern, _, _, _, _}}]},
                  publish,
                  [{pattern, [<<"%u">>]},  {pattern, [<<"%c">>]}]
                 }, emqx_authz_rule:compile(?RULE5)),
    ok.


t_match(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo2 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {192,168,1,10},
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

    ?assertEqual({matched, deny},
                emqx_authz_rule:match(ClientInfo1, subscribe, <<"#">>, emqx_authz_rule:compile(?RULE1))),
    ?assertEqual({matched, deny},
                emqx_authz_rule:match(ClientInfo2, subscribe, <<"+">>, emqx_authz_rule:compile(?RULE1))),
    ?assertEqual({matched, deny},
                emqx_authz_rule:match(ClientInfo3, subscribe, <<"topic/test">>, emqx_authz_rule:compile(?RULE1))),

    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo1, subscribe, <<"#">>, emqx_authz_rule:compile(?RULE2))),
    ?assertEqual(nomatch,
                emqx_authz_rule:match(ClientInfo1, subscribe, <<"topic/test">>, emqx_authz_rule:compile(?RULE2))),
    ?assertEqual(nomatch,
                emqx_authz_rule:match(ClientInfo2, subscribe, <<"#">>, emqx_authz_rule:compile(?RULE2))),

    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo1, subscribe, <<"test">>, emqx_authz_rule:compile(?RULE3))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo2, subscribe, <<"test">>, emqx_authz_rule:compile(?RULE3))),
    ?assertEqual(nomatch,
                emqx_authz_rule:match(ClientInfo2, subscribe, <<"topic/test">>, emqx_authz_rule:compile(?RULE3))),

    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo1, publish, <<"topic/test">>, emqx_authz_rule:compile(?RULE4))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo2, publish, <<"topic/test">>, emqx_authz_rule:compile(?RULE4))),
    ?assertEqual(nomatch,
                emqx_authz_rule:match(ClientInfo3, publish, <<"topic/test">>, emqx_authz_rule:compile(?RULE4))),
    ?assertEqual(nomatch,
                emqx_authz_rule:match(ClientInfo4, publish, <<"topic/test">>, emqx_authz_rule:compile(?RULE4))),

    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo1, publish, <<"test">>, emqx_authz_rule:compile(?RULE5))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo2, publish, <<"test">>, emqx_authz_rule:compile(?RULE5))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo3, publish, <<"test">>, emqx_authz_rule:compile(?RULE5))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo3, publish, <<"fake">>, emqx_authz_rule:compile(?RULE5))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo4, publish, <<"test">>, emqx_authz_rule:compile(?RULE5))),
    ?assertEqual({matched, allow},
                emqx_authz_rule:match(ClientInfo4, publish, <<"fake">>, emqx_authz_rule:compile(?RULE5))),

    ok.

