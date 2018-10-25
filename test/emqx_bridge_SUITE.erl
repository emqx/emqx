%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [bridge_test].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

bridge_test(_) ->
    {ok, _Pid} = emqx_bridge:start_link(emqx, []),
    #{msg := <<"start bridge successfully">>}
        = emqx_bridge:start_bridge(emqx),
    test_forwards(),
    test_subscriptions(0),
    test_subscriptions(1),
    test_subscriptions(2),
    #{msg := <<"stop bridge successfully">>}
        = emqx_bridge:stop_bridge(emqx),
    ok.

test_forwards() ->
    emqx_bridge:add_forward(emqx, <<"test_forwards">>),
    [<<"test_forwards">>] = emqx_bridge:show_forwards(emqx),
    emqx_bridge:del_forward(emqx, <<"test_forwards">>),
    [] = emqx_bridge:show_forwards(emqx),
    ok.

test_subscriptions(QoS) ->
    emqx_bridge:add_subscription(emqx, <<"test_subscriptions">>, QoS),
    [{<<"test_subscriptions">>, QoS}] = emqx_bridge:show_subscriptions(emqx),
    emqx_bridge:del_subscription(emqx, <<"test_subscriptions">>),
    [] = emqx_bridge:show_subscriptions(emqx),
    ok.
