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

-module(emqx_authz_mongo_SUITE).

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
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end ),

    %% important! let emqx_schema include the current app!
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, includes, fun() -> ["emqx_authz"] end ),

    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    ct:pal("---- emqx_hooks: ~p", [ets:tab2list(emqx_hooks)]),
    ok = emqx_config:update_config([zones, default, acl, cache, enable], false),
    ok = emqx_config:update_config([zones, default, acl, enable], true),
    Rules = [#{ <<"config">> => #{
                        <<"mongo_type">> => <<"single">>,
                        <<"server">> => <<"127.0.0.1:27017">>,
                        <<"pool_size">> => 1,
                        <<"database">> => <<"mqtt">>,
                        <<"ssl">> => #{<<"enable">> => false}},
                <<"principal">> => <<"all">>,
                <<"collection">> => <<"fake">>,
                <<"find">> => #{<<"a">> => <<"b">>},
                <<"type">> => <<"mongo">>}
            ],
    ok = emqx_authz:update(replace, Rules),
    Config.

end_per_suite(_Config) ->
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'authz.conf')),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_resource]),
    meck:unload(emqx_schema),
    meck:unload(emqx_resource).

-define(RULE1,[#{<<"topics">> => [<<"#">>],
                 <<"permission">> => <<"deny">>,
                 <<"action">> => <<"all">>}]).
-define(RULE2,[#{<<"topics">> => [<<"eq #">>],
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"all">>}]).
-define(RULE3,[#{<<"topics">> => [<<"test/%c">>],
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"subscribe">>}]).
-define(RULE4,[#{<<"topics">> => [<<"test/%u">>],
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"publish">>}]).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_) ->
    ClientInfo1 = #{clientid => <<"test">>,
                    username => <<"test">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo2 = #{clientid => <<"test_clientid">>,
                    username => <<"test_username">>,
                    peerhost => {192,168,0,10},
                    zone => default,
                    listener => mqtt_tcp
                   },
    ClientInfo3 = #{clientid => <<"test_clientid">>,
                    username => <<"fake_username">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => mqtt_tcp
                   },

    meck:expect(emqx_resource, query, fun(_, _) -> [] end),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"#">>)), % nomatch
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, publish, <<"#">>)), % nomatch

    meck:expect(emqx_resource, query, fun(_, _) -> ?RULE1 ++ ?RULE2 end),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"+">>)),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, publish, <<"+">>)),

    meck:expect(emqx_resource, query, fun(_, _) -> ?RULE2 ++ ?RULE1 end),
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo1, subscribe, <<"#">>)),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"+">>)),

    meck:expect(emqx_resource, query, fun(_, _) -> ?RULE3 ++ ?RULE4 end),
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo2, subscribe, <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(ClientInfo2, publish,   <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(ClientInfo2, subscribe, <<"test/test_username">>)),
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo2, publish,   <<"test/test_username">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(ClientInfo3, subscribe, <<"test">>)), % nomatch
    ?assertEqual(deny,  emqx_access_control:authorize(ClientInfo3, publish,   <<"test">>)), % nomatch
    ok.

