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

-module(emqx_rule_id).

-export([gen/0, gen/1]).

-define(SHORT, 8).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec(gen() -> list()).
gen() ->
    gen(?SHORT).

-spec(gen(integer()) -> list()).
gen(Len) ->
    BitLen = Len * 4,
    <<R:BitLen>> = crypto:strong_rand_bytes(Len div 2),
    int_to_hex(R, Len).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

int_to_hex(I, N) when is_integer(I), I >= 0 ->
    int_to_hex([], I, 1, N).

int_to_hex(L, I, Count, N)
    when I < 16 ->
    pad([int_to_hex(I) | L], N - Count);
int_to_hex(L, I, Count, N) ->
    int_to_hex([int_to_hex(I rem 16) | L], I div 16, Count + 1, N).

int_to_hex(I) when 0 =< I, I =< 9 ->
    I + $0;
int_to_hex(I) when 10 =< I, I =< 15 ->
    (I - 10) + $a.

pad(L, 0) ->
    L;
pad(L, Count) ->
    pad([$0 | L], Count - 1).
