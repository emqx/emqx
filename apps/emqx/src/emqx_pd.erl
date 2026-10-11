%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The utility functions for erlang process dictionary.
-module(emqx_pd).

-include("types.hrl").

-export([
    get_counters/1,
    get_counter/1,
    inc_counter/2,
    inc_counter_with_delta/2,
    get_counter_delta/1,
    take_counter_delta/1,
    reset_counter/1
]).

-compile(
    {inline, [
        get_counters/1,
        get_counter/1,
        inc_counter/2,
        inc_counter_with_delta/2,
        get_counter_delta/1,
        take_counter_delta/1,
        reset_counter/1
    ]}
).

-type key() :: term().

-spec get_counters(list(key())) -> list({key(), number()}).
get_counters(Keys) when is_list(Keys) ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- Keys].

-spec get_counter(key()) -> number().
get_counter(Key) ->
    case get(Key) of
        undefined -> 0;
        {Counter, Delta} -> Counter + Delta;
        Cnt -> Cnt
    end.

-spec inc_counter(key(), number()) -> option(number()).
inc_counter(Key, Inc) ->
    case get(Key) of
        {Counter, Delta} ->
            put(Key, {Counter + Inc, Delta}),
            Counter + Delta;
        undefined ->
            put(Key, Inc);
        Counter ->
            put(Key, Counter + Inc)
    end.

-doc "Increment a counter and retain its unreported delta in the same dictionary entry.".
-spec inc_counter_with_delta(key(), number()) -> option(number()).
inc_counter_with_delta(Key, Inc) ->
    case get(Key) of
        {Counter, Delta} ->
            put(Key, {Counter, Delta + Inc}),
            Counter + Delta;
        undefined ->
            put(Key, {0, Inc}),
            undefined;
        Counter ->
            put(Key, {Counter, Inc})
    end.

-spec get_counter_delta(key()) -> number().
get_counter_delta(Key) ->
    case get(Key) of
        {_, Delta} -> Delta;
        _ -> 0
    end.

-doc "Return the unreported delta and compact the entry to its cumulative counter value.".
-spec take_counter_delta(key()) -> number().
take_counter_delta(Key) ->
    case get(Key) of
        {Counter, Delta} ->
            put(Key, Counter + Delta),
            Delta;
        _ ->
            0
    end.

-spec reset_counter(key()) -> number().
reset_counter(Key) ->
    case put(Key, 0) of
        undefined -> 0;
        {Counter, Delta} -> Counter + Delta;
        Cnt -> Cnt
    end.
