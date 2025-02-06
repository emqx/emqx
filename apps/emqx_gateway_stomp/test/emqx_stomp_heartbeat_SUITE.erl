%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_heartbeat_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_init(_) ->
    #{} = emqx_stomp_heartbeat:init({0, 0}),
    #{incoming := _} = emqx_stomp_heartbeat:init({1, 0}),
    #{outgoing := _} = emqx_stomp_heartbeat:init({0, 1}).

t_check_1(_) ->
    HrtBt = emqx_stomp_heartbeat:init({1, 1}),
    {ok, HrtBt1} = emqx_stomp_heartbeat:check(incoming, 0, HrtBt),
    {error, timeout} = emqx_stomp_heartbeat:check(incoming, 0, HrtBt1),

    {ok, HrtBt2} = emqx_stomp_heartbeat:check(outgoing, 0, HrtBt1),
    {error, timeout} = emqx_stomp_heartbeat:check(outgoing, 0, HrtBt2),
    ok.

t_check_2(_) ->
    HrtBt = emqx_stomp_heartbeat:init({1, 0}),
    #{incoming := _} = lists:foldl(
        fun(I, Acc) ->
            {ok, NAcc} = emqx_stomp_heartbeat:check(incoming, I, Acc),
            NAcc
        end,
        HrtBt,
        lists:seq(1, 1000)
    ),
    ok.

t_info(_) ->
    HrtBt = emqx_stomp_heartbeat:init({100, 100}),
    #{
        incoming := _,
        outgoing := _
    } = emqx_stomp_heartbeat:info(HrtBt).

t_interval(_) ->
    HrtBt = emqx_stomp_heartbeat:init({1, 0}),
    1 = emqx_stomp_heartbeat:interval(incoming, HrtBt),
    undefined = emqx_stomp_heartbeat:interval(outgoing, HrtBt).

t_reset(_) ->
    %% initial state
    Hb0 = emqx_stomp_heartbeat:init({10, 0}),
    %% timer trigger a check before any packet is received (statval = 0)
    {ok, Hb1} = emqx_stomp_heartbeat:check(incoming, 0, Hb0),
    %% timer triggered again, statval is still 0
    %% this time the check should result in {error, timeout}
    ?assertEqual({error, timeout}, emqx_stomp_heartbeat:check(incoming, 0, Hb1)),
    %% If a heartbeat is received before timer expire (trigger reset)
    %% heartbeat itself is a packet, so 'statval' becomes 1
    Hb2 = emqx_stomp_heartbeat:reset(incoming, 1, Hb1),
    %% timer triggers a check, should pass
    {ok, Hb3} = emqx_stomp_heartbeat:check(incoming, 1, Hb2),
    %% timer triggers a check again, no new packet, no heartbeat (statval = 1)
    %% expect {error, timeout}
    ?assertEqual({error, timeout}, emqx_stomp_heartbeat:check(incoming, 1, Hb3)),
    ok.
