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

-module(emqx_psk_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    meck:new(emqx_config, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_config, get, fun([psk_authentication, enable]) -> true;
                                     ([psk_authentication, chunk_size]) -> 50;
                                     (KeyPath) -> meck:passthrough([KeyPath])
                                  end),
    meck:expect(emqx_config, get, fun([psk_authentication, init_file], _) ->
                                         filename:join([code:lib_dir(emqx_psk, test), "data/init.psk"]);
                                     ([psk_authentication, separator], _) -> <<":">>;
                                     (KeyPath, Default) -> meck:passthrough([KeyPath, Default])
                                  end),
    emqx_common_test_helpers:start_apps([emqx_psk]),
    Config.

end_per_suite(_) ->
    meck:unload(emqx_config),
    emqx_common_test_helpers:stop_apps([emqx_psk]),
    ok.

t_psk_lookup(_) ->

    PSKIdentity1 = <<"myclient1">>,
    SharedSecret1 = <<"8c701116e9127c57a99d5563709af3deaca75563e2c4dd0865701ae839fb6d79">>,
    ?assertEqual({stop, {ok, SharedSecret1}}, emqx_psk:on_psk_lookup(PSKIdentity1, any)),

    PSKIdentity2 = <<"myclient2">>,
    SharedSecret2 = <<"d1e617d3b963757bfc21dad3fea169716c3a2f053f23decaea5cdfaabd04bfc4">>,
    ?assertEqual({stop, {ok, SharedSecret2}}, emqx_psk:on_psk_lookup(PSKIdentity2, any)),

    ?assertEqual(ignore, emqx_psk:on_psk_lookup(<<"myclient3">>, any)),

    ClientLookup = fun(psk, undefined, _) -> {ok, SharedSecret1};
                      (psk, _, _) -> error
                   end,

    ClientTLSOpts = #{ versions => ['tlsv1.2']
                     , ciphers => ["PSK-AES256-CBC-SHA"]
                     , psk_identity => "myclient1"
                     , verify => verify_none
                     , user_lookup_fun => {ClientLookup, undefined}
                     },

    ServerTLSOpts = #{ versions => ['tlsv1.2']
                     , ciphers => ["PSK-AES256-CBC-SHA"]
                     , verify => verify_none
                     , reuseaddr => true
                     , user_lookup_fun => {fun emqx_tls_psk:lookup/3, undefined}
                     },
    emqx_config:put([listeners, ssl ,default, ssl], ServerTLSOpts),
    emqx_listeners:restart_listener('ssl:default'),

    {ok, Socket} = ssl:connect("127.0.0.1", 8883, maps:to_list(ClientTLSOpts)),
    ssl:close(Socket),

    ClientTLSOpts1 = ClientTLSOpts#{psk_identity => "myclient2"},
    ?assertMatch({error, _}, ssl:connect("127.0.0.1", 8883, maps:to_list(ClientTLSOpts1))),

    ok.

