%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_rate(_) ->
    %% infinity
    ?assertEqual({ok, infinity}, to_rate(" infinity ")),

    %% xMB
    ?assertEqual({ok, {100, 1000}}, to_rate("100")),
    ?assertEqual({ok, {100, 1000}}, to_rate("  100   ")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_rate("100MB")),

    %% xMB/s
    ?assertEqual({ok, {100, 1000}}, to_rate("100/s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_rate("100MB/s")),

    %% xMB/ys
    ?assertEqual({ok, {100, 10000}}, to_rate("100/10s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 10000}}, to_rate("100mB/10s")),

    ?assertMatch({error, _}, to_rate("infini")),
    ?assertMatch({error, _}, to_rate("0")),
    ?assertMatch({error, _}, to_rate("MB")),
    ?assertMatch({error, _}, to_rate("10s")),
    ?assertMatch({error, _}, to_rate("100MB/")),
    ?assertMatch({error, _}, to_rate("100MB/xx")),
    ?assertMatch({error, _}, to_rate("100MB/1")),
    ?assertMatch({error, _}, to_rate("100/10x")),

    ok.

t_burst(_) ->
    %% Zero is valid for burst
    ?assertMatch({ok, {0, 1000}}, to_burst("0")),

    %% xMB
    ?assertEqual({ok, {100, 1000}}, to_burst("100")),
    ?assertEqual({ok, {100, 1000}}, to_burst("  100   ")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_burst("100MB")),

    %% xMB/s
    ?assertEqual({ok, {100, 1000}}, to_burst("100/s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_burst("100MB/s")),

    %% xMB/ys
    ?assertEqual({ok, {100, 10000}}, to_burst("100/10s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 10000}}, to_burst("100mB/10s")),

    %% burst cannot be infinity
    ?assertMatch({error, _}, to_burst("infinity")),
    ?assertMatch({error, _}, to_burst("infini")),
    ?assertMatch({error, _}, to_burst("MB")),
    ?assertMatch({error, _}, to_burst("10s")),
    ?assertMatch({error, _}, to_burst("100MB/")),
    ?assertMatch({error, _}, to_burst("100MB/xx")),
    ?assertMatch({error, _}, to_burst("100MB/1")),
    ?assertMatch({error, _}, to_burst("100/10x")),

    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

to_rate(Str) ->
    emqx_limiter_schema:to_rate(Str).

to_burst(Str) ->
    emqx_limiter_schema:to_burst(Str).
