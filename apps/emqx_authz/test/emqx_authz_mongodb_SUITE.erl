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

-module(emqx_authz_mongodb_SUITE).

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
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

    ok = emqx_common_test_helpers:start_apps(
           [emqx_conf, emqx_authz],
           fun set_special_configs/1
          ),

    Rules = [#{<<"type">> => <<"mongodb">>,
               <<"mongo_type">> => <<"single">>,
               <<"server">> => <<"127.0.0.1:27017">>,
               <<"pool_size">> => 1,
               <<"database">> => <<"mqtt">>,
               <<"ssl">> => #{<<"enable">> => false},
               <<"collection">> => <<"fake">>,
               <<"selector">> => #{<<"a">> => <<"b">>}
              }],
    {ok, _} = emqx_authz:update(replace, Rules),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
                [authorization],
                #{<<"no_match">> => <<"allow">>,
                  <<"cache">> => #{<<"enable">> => <<"true">>},
                  <<"sources">> => []}),
    emqx_common_test_helpers:stop_apps([emqx_authz, emqx_conf]),
    meck:unload(emqx_resource),
    ok.

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

-define(SOURCE1,[#{<<"topics">> => [<<"#">>],
                 <<"permission">> => <<"deny">>,
                 <<"action">> => <<"all">>}]).
-define(SOURCE2,[#{<<"topics">> => [<<"eq #">>],
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"all">>}]).
-define(SOURCE3,[#{<<"topics">> => [<<"test/", ?PH_CLIENTID/binary>>],
                 <<"permission">> => <<"allow">>,
                 <<"action">> => <<"subscribe">>}]).
-define(SOURCE4,[#{<<"topics">> => [<<"test/", ?PH_USERNAME/binary>>],
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
                    listener => {tcp, default}
                   },
    ClientInfo2 = #{clientid => <<"test_clientid">>,
                    username => <<"test_username">>,
                    peerhost => {192,168,0,10},
                    zone => default,
                    listener => {tcp, default}
                   },
    ClientInfo3 = #{clientid => <<"test_clientid">>,
                    username => <<"fake_username">>,
                    peerhost => {127,0,0,1},
                    zone => default,
                    listener => {tcp, default}
                   },

    meck:expect(emqx_resource, query, fun(_, _) -> [] end),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"#">>)), % nomatch
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, publish, <<"#">>)), % nomatch

    meck:expect(emqx_resource, query, fun(_, _) -> ?SOURCE1 ++ ?SOURCE2 end),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"+">>)),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, publish, <<"+">>)),

    meck:expect(emqx_resource, query, fun(_, _) -> ?SOURCE2 ++ ?SOURCE1 end),
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo1, subscribe, <<"#">>)),
    ?assertEqual(deny, emqx_access_control:authorize(ClientInfo1, subscribe, <<"+">>)),

    meck:expect(emqx_resource, query, fun(_, _) -> ?SOURCE3 ++ ?SOURCE4 end),
    ?assertEqual(allow, emqx_access_control:authorize(
                          ClientInfo2, subscribe, <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(
                          ClientInfo2, publish,   <<"test/test_clientid">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(
                          ClientInfo2, subscribe, <<"test/test_username">>)),
    ?assertEqual(allow, emqx_access_control:authorize(
                          ClientInfo2, publish,   <<"test/test_username">>)),
    ?assertEqual(deny,  emqx_access_control:authorize(
                          ClientInfo3, subscribe, <<"test">>)), % nomatch
    ?assertEqual(deny,  emqx_access_control:authorize(
                          ClientInfo3, publish,   <<"test">>)), % nomatch
    ok.
