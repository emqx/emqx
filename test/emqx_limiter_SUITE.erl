%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_, Cfg) ->
    _ = esockd_limiter:start_link(),
    Cfg.

end_per_testcase(_, _) ->
    esockd_limiter:stop().

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_init(_) ->
    Cap1 = 1000, Intv1 = 10,
    Cap2 = 2000, Intv2 = 15,
    undefined = emqx_limiter:init(external, undefined, undefined, []),
    #{conn_bytes_in := #{capacity := Cap2, interval := Intv2, tokens := Cap2},
      conn_messages_in := #{capacity := Cap1, interval := Intv1, tokens := Cap1 }} =
        emqx_limiter:info(
          emqx_limiter:init(external, {Cap1, Intv1}, {Cap2, Intv2}, [])),
    #{conn_bytes_in := #{capacity := Cap2, interval := Intv2, tokens := Cap2 }} =
        emqx_limiter:info(
          emqx_limiter:init(external, undefined, {Cap1, Intv1}, [{conn_bytes_in, {Cap2, Intv2}}])).

t_check_conn(_) ->
    Limiter = emqx_limiter:init(external, [{conn_bytes_in, {100, 1}}]),

    {ok, Limiter2} = emqx_limiter:check(#{cnt => 0, oct => 1}, Limiter),
    #{conn_bytes_in := #{tokens := 99}} = emqx_limiter:info(Limiter2),

    {pause, 10, Limiter3} = emqx_limiter:check(#{cnt => 0, oct => 100}, Limiter),
    #{conn_bytes_in := #{tokens := 0}} = emqx_limiter:info(Limiter3),

    {pause, 100000, Limiter4} = emqx_limiter:check(#{cnt => 0, oct => 10000}, Limiter3),
    #{conn_bytes_in := #{tokens := 0}} = emqx_limiter:info(Limiter4).

t_check_overall(_) ->
    Limiter = emqx_limiter:init(external, [{overall_messages_routing, {100, 1}}]),

    {ok, Limiter2} = emqx_limiter:check(#{cnt => 1, oct => 0}, Limiter),
    #{overall_messages_routing := #{tokens := 99}} = emqx_limiter:info(Limiter2),

    %% XXX: P = 1/r = 1/100 * 1000 = 10ms ?
    {pause, _, Limiter3} = emqx_limiter:check(#{cnt => 100, oct => 0}, Limiter),
    #{overall_messages_routing := #{tokens := 0}} = emqx_limiter:info(Limiter2),

    %% XXX: P = 10000/r = 10000/100 * 1000 = 100s ?
    {pause, _, Limiter4} = emqx_limiter:check(#{cnt => 10000, oct => 0}, Limiter3),
    #{overall_messages_routing := #{tokens := 0}} = emqx_limiter:info(Limiter4).

