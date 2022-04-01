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

-module(emqx_authz_rule_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(SOURCE1, {deny, all}).
-define(SOURCE2, {allow, {ipaddr, "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]}).
-define(SOURCE3, {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, [?PH_S_CLIENTID]}).
-define(SOURCE4, {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}).
-define(SOURCE5,
    {allow,
        {'or', [
            {username, {re, "^test"}},
            {clientid, {re, "test?"}}
        ]},
        publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([emqx_authz, emqx_conf]),
    ok.

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

t_compile(_) ->
    ?assertEqual({deny, all, all, [['#']]}, emqx_authz_rule:compile(?SOURCE1)),

    ?assertEqual(
        {allow, {ipaddr, {{127, 0, 0, 1}, {127, 0, 0, 1}, 32}}, all, [{eq, ['#']}, {eq, ['+']}]},
        emqx_authz_rule:compile(?SOURCE2)
    ),

    ?assertEqual(
        {allow,
            {ipaddrs, [
                {{127, 0, 0, 1}, {127, 0, 0, 1}, 32},
                {{192, 168, 1, 0}, {192, 168, 1, 255}, 24}
            ]},
            subscribe, [{pattern, [?PH_CLIENTID]}]},
        emqx_authz_rule:compile(?SOURCE3)
    ),

    ?assertMatch(
        {allow, {'and', [{clientid, {eq, <<"test">>}}, {username, {eq, <<"test">>}}]}, publish, [
            [<<"topic">>, <<"test">>]
        ]},
        emqx_authz_rule:compile(?SOURCE4)
    ),

    ?assertMatch(
        {allow,
            {'or', [
                {username, {re_pattern, _, _, _, _}},
                {clientid, {re_pattern, _, _, _, _}}
            ]},
            publish, [{pattern, [?PH_USERNAME]}, {pattern, [?PH_CLIENTID]}]},
        emqx_authz_rule:compile(?SOURCE5)
    ),
    ok.

t_match(_) ->
    ClientInfo1 = #{
        clientid => <<"test">>,
        username => <<"test">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },
    ClientInfo2 = #{
        clientid => <<"test">>,
        username => <<"test">>,
        peerhost => {192, 168, 1, 10},
        zone => default,
        listener => {tcp, default}
    },
    ClientInfo3 = #{
        clientid => <<"test">>,
        username => <<"fake">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },
    ClientInfo4 = #{
        clientid => <<"fake">>,
        username => <<"test">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            ClientInfo1,
            subscribe,
            <<"#">>,
            emqx_authz_rule:compile(?SOURCE1)
        )
    ),
    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            ClientInfo2,
            subscribe,
            <<"+">>,
            emqx_authz_rule:compile(?SOURCE1)
        )
    ),
    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            ClientInfo3,
            subscribe,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE1)
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo1,
            subscribe,
            <<"#">>,
            emqx_authz_rule:compile(?SOURCE2)
        )
    ),
    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            ClientInfo1,
            subscribe,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE2)
        )
    ),
    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            ClientInfo2,
            subscribe,
            <<"#">>,
            emqx_authz_rule:compile(?SOURCE2)
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo1,
            subscribe,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE3)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo2,
            subscribe,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE3)
        )
    ),
    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            ClientInfo2,
            subscribe,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE3)
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo1,
            publish,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE4)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo2,
            publish,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE4)
        )
    ),
    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            ClientInfo3,
            publish,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE4)
        )
    ),
    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            ClientInfo4,
            publish,
            <<"topic/test">>,
            emqx_authz_rule:compile(?SOURCE4)
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo1,
            publish,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo2,
            publish,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo3,
            publish,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo3,
            publish,
            <<"fake">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo4,
            publish,
            <<"test">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            ClientInfo4,
            publish,
            <<"fake">>,
            emqx_authz_rule:compile(?SOURCE5)
        )
    ),
    ok.
