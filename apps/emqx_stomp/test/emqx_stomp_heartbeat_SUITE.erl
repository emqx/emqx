%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_ct:all(?MODULE).

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

    {error, timeout} = emqx_stomp_heartbeat:check(outgoing, 0, HrtBt1),
    ok.

t_check_2(_) ->
    HrtBt = emqx_stomp_heartbeat:init({1, 0}),
    #{incoming := _} = lists:foldl(fun(I, Acc) ->
                            {ok, NAcc} = emqx_stomp_heartbeat:check(incoming, I, Acc),
                            NAcc
                       end, HrtBt, lists:seq(1,1000)),
    ok.

t_info(_) ->
    HrtBt = emqx_stomp_heartbeat:init({100, 100}),
    #{incoming := _,
      outgoing := _} = emqx_stomp_heartbeat:info(HrtBt).

t_interval(_) ->
    HrtBt = emqx_stomp_heartbeat:init({1, 0}),
    1 = emqx_stomp_heartbeat:interval(incoming, HrtBt),
    undefined = emqx_stomp_heartbeat:interval(outgoing, HrtBt).

