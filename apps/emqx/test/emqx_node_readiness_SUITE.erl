%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_node_readiness_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_node_readiness:mark_ready().

%% Checks that the node is ready by default, so contexts that do not
%% boot through emqx_machine (e.g. test suites) are not gated.
t_ready_by_default(_Config) ->
    _ = persistent_term:erase({emqx_node_readiness, ready}),
    ?assert(emqx_node_readiness:is_ready()).

%% Checks that mark_not_ready/0 and mark_ready/0 toggle the flag.
t_mark_ready_toggle(_Config) ->
    ok = emqx_node_readiness:mark_not_ready(),
    ?assertNot(emqx_node_readiness:is_ready()),
    ok = emqx_node_readiness:mark_ready(),
    ?assert(emqx_node_readiness:is_ready()).

%% Checks that a TCP MQTT connection is refused while the node is not
%% ready, and accepted again once it is ready.
t_tcp_connection_gated(_Config) ->
    process_flag(trap_exit, true),
    ok = emqx_node_readiness:mark_not_ready(),
    {ok, C1} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
    ?assertMatch({error, _}, emqtt:connect(C1)),
    ok = emqx_node_readiness:mark_ready(),
    {ok, C2} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
    ?assertMatch({ok, _}, emqtt:connect(C2)),
    ok = emqtt:disconnect(C2).

%% Checks that a WebSocket MQTT connection is refused while the node is
%% not ready, and accepted again once it is ready.
t_ws_connection_gated(_Config) ->
    process_flag(trap_exit, true),
    ok = emqx_node_readiness:mark_not_ready(),
    {ok, C1} = emqtt:start_link([{host, "127.0.0.1"}, {port, 8083}]),
    ?assertMatch({error, _}, emqtt:ws_connect(C1)),
    ok = emqx_node_readiness:mark_ready(),
    {ok, C2} = emqtt:start_link([{host, "127.0.0.1"}, {port, 8083}]),
    ?assertMatch({ok, _}, emqtt:ws_connect(C2)),
    ok = emqtt:disconnect(C2).
