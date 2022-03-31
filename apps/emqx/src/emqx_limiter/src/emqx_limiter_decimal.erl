%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% a simple decimal module for rate-related calculations

-module(emqx_limiter_decimal).

%% API
-export([
    add/2,
    sub/2,
    mul/2,
    put_to_counter/3,
    floor_div/2
]).
-export_type([decimal/0, zero_or_float/0]).

-type decimal() :: infinity | number().
-type zero_or_float() :: 0 | float().

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
-spec add(decimal(), decimal()) -> decimal().
add(A, B) when
    A =:= infinity orelse
        B =:= infinity
->
    infinity;
add(A, B) ->
    A + B.

-spec sub(decimal(), decimal()) -> decimal().
sub(A, B) when
    A =:= infinity orelse
        B =:= infinity
->
    infinity;
sub(A, B) ->
    A - B.

-spec mul(decimal(), decimal()) -> decimal().
mul(A, B) when
    A =:= infinity orelse
        B =:= infinity
->
    infinity;
mul(A, B) ->
    A * B.

-spec floor_div(decimal(), number()) -> decimal().
floor_div(infinity, _) ->
    infinity;
floor_div(A, B) ->
    erlang:floor(A / B).

-spec put_to_counter(counters:counters_ref(), pos_integer(), decimal()) -> ok.
put_to_counter(_, _, infinity) ->
    ok;
put_to_counter(Counter, Index, Val) when is_float(Val) ->
    IntPart = erlang:floor(Val),
    counters:put(Counter, Index, IntPart);
put_to_counter(Counter, Index, Val) ->
    counters:put(Counter, Index, Val).
