%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_cm_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_is_enabled(_) ->
    application:set_env(emqx, enable_session_registry, false),
    ?assertEqual(false, emqx_cm_registry:is_enabled()),
    application:set_env(emqx, enable_session_registry, true),
    ?assertEqual(true, emqx_cm_registry:is_enabled()).

t_register_unregister_channel(_) ->
    ClientId = <<"clientid">>,
    application:set_env(emqx, enable_session_registry, false),
    emqx_cm_registry:register_channel(ClientId),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId)),

    application:set_env(emqx, enable_session_registry, true),
    emqx_cm_registry:register_channel(ClientId),
    ?assertEqual([self()], emqx_cm_registry:lookup_channels(ClientId)),

    application:set_env(emqx, enable_session_registry, false),
    emqx_cm_registry:unregister_channel(ClientId),
    ?assertEqual([self()], emqx_cm_registry:lookup_channels(ClientId)),

    application:set_env(emqx, enable_session_registry, true),
    emqx_cm_registry:unregister_channel(ClientId),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId)).

t_cleanup_channels(_) ->
    ClientId = <<"clientid">>,
    ClientId2 = <<"clientid2">>,
    emqx_cm_registry:register_channel(ClientId),
    emqx_cm_registry:register_channel(ClientId2),
    ?assertEqual([self()], emqx_cm_registry:lookup_channels(ClientId)),
    emqx_cm_registry ! {membership, {mnesia, down, node()}},
    ct:sleep(100),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId)),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId2)).
