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

-module(emqx_authz_pgsql_SUITE).

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
    meck:new(emqx_resource, [non_strict, passthrough]),
    meck:expect(emqx_resource, check_and_create_local, fun(_, _, _) -> {ok, meck_data} end ),
    ok = emqx_ct_helpers:start_apps([emqx_authz], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_resource]),
    meck:unload(emqx_resource).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, acl_nomatch, deny),
    ok;
set_special_configs(emqx_authz) ->
    application:set_env(emqx_authz, rules,
                        [#{<<"config">> =>#{},
                           <<"principal">> => all,
                           <<"sql">> => <<"fake sql">>,
                           <<"type">> => pgsql}
                        ]),
    ok;
set_special_configs(_App) ->
    ok.

-define(COLUMNS, [ {column, <<"access">>, meck, meck, meck, meck, meck, meck, meck}
                 , {column, <<"ipaddress">>, meck, meck, meck, meck, meck, meck, meck}
                 , {column, <<"username">>, meck, meck, meck, meck, meck, meck, meck}
                 , {column, <<"clientid">>, meck, meck, meck, meck, meck, meck, meck}
                 , {column, <<"action">>, meck, meck, meck, meck, meck, meck, meck}
                 , {column, <<"topic">>, meck, meck, meck, meck, meck, meck, meck}
                 ]).
-define(RULE1, [{0,null,<<"$all">>,null,3,<<"#">>}]).
-define(RULE2, [{1,<<"127.0.0.1">>,null,null,3,<<"eq #">>}]).
-define(RULE3, [{1,null,<<"^test?">>,<<"^test?">>,2,<<"test">>}]).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    zone => zone
                   },
    ClientInfo2 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {192,168,0,10},
                    zone => zone
                   },
    ClientInfo3 = #{clientid => <<"test">>,
                    username => <<"fake">>,
                    zone => zone
                   },

    meck:expect(emqx_resource, query,
                fun(_, _) -> {ok, ?COLUMNS, []}
                end),
    % nomatch
    ?assertEqual(deny,
                 emqx_access_control:check_acl(#{zone => zone}, subscribe, <<"#">>)),
    % nomatch
    ?assertEqual(deny,
                 emqx_access_control:check_acl(#{zone => zone}, publish, <<"#">>)),

    meck:expect(emqx_resource, query,
                fun(_, _) -> {ok, ?COLUMNS, ?RULE1 ++ ?RULE2}
                end),
    ?assertEqual(deny,
        emqx_access_control:check_acl(ClientInfo1, subscribe, <<"+">>)),

    meck:expect(emqx_resource, query,
                fun(_, _) -> {ok, ?COLUMNS, ?RULE2 ++ ?RULE1}
                end),
    ?assertEqual(allow,
        emqx_access_control:check_acl(ClientInfo1, subscribe, <<"#">>)),
    ?assertEqual(deny,
        emqx_access_control:check_acl(ClientInfo2, subscribe, <<"#">>)),

    meck:expect(emqx_resource, query,
                fun(_, _) -> {ok, ?COLUMNS, ?RULE3}
                end),
    ?assertEqual(allow,
        emqx_access_control:check_acl(ClientInfo2, publish, <<"test">>)),
    % nomatch
    ?assertEqual(deny,
        emqx_access_control:check_acl(ClientInfo3, publish, <<"test">>)),
    ok.

