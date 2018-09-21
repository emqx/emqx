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

%% @doc This module manages an opaque collection of statistics data used to
%% force garbage collection on `self()' process when hitting thresholds.
%% Namely:
%% (1) Total number of messages passed through
%% (2) Total data volume passed through
%% @end

-module(emqx_gc).

-export([init/1, inc/2, reset/0]).

-type st() :: #{ cnt => {integer(), integer()}
               , oct => {integer(), integer()}
               }.

-define(disabled, disabled).
-define(ENABLED(X), (is_integer(X) andalso X > 0)).

%% @doc Initialize force GC parameters.
-spec init(false | map()) -> ok.
init(#{count := Count, bytes := Bytes}) ->
    Cnt = [{cnt, {Count, Count}} || ?ENABLED(Count)],
    Oct = [{oct, {Bytes, Bytes}} || ?ENABLED(Bytes)],
    erlang:put(?MODULE, maps:from_list(Cnt ++ Oct)),
    ok;
init(_) -> erlang:put(?MODULE, #{}), ok.

%% @doc Increase count and bytes stats in one call,
%% ensure gc is triggered at most once, even if both thresholds are hit.
-spec inc(pos_integer(), pos_integer()) -> ok.
inc(Cnt, Oct) ->
    mutate_pd_with(fun(St) -> inc(St, Cnt, Oct) end).

%% @doc Reset counters to zero.
-spec reset() -> ok.
reset() ->
    mutate_pd_with(fun(St) -> reset(St) end).

%% ======== Internals ========

%% mutate gc stats numbers in process dict with the given function
mutate_pd_with(F) ->
    St = F(erlang:get(?MODULE)),
    erlang:put(?MODULE, St),
    ok.

%% Increase count and bytes stats in one call,
%% ensure gc is triggered at most once, even if both thresholds are hit.
-spec inc(st(), pos_integer(), pos_integer()) -> st().
inc(St0, Cnt, Oct) ->
    case do_inc(St0, cnt, Cnt) of
        {true, St} ->
            St;
        {false, St1} ->
            {_, St} = do_inc(St1, oct, Oct),
            St
    end.

%% Reset counters to zero.
reset(St) -> reset(cnt, reset(oct, St)).

-spec do_inc(st(), cnt | oct, pos_integer()) -> {boolean(), st()}.
do_inc(St, Key, Num) ->
    case maps:get(Key, St, ?disabled) of
        ?disabled ->
            {false, St};
        {Init, Remain} when Remain > Num ->
            {false, maps:put(Key, {Init, Remain - Num}, St)};
        _ ->
            {true, do_gc(St)}
    end.

do_gc(St) ->
    erlang:garbage_collect(),
    reset(St).

reset(Key, St) ->
    case maps:get(Key, St, ?disabled) of
        ?disabled -> St;
        {Init, _} -> maps:put(Key, {Init, Init}, St)
    end.

