%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The utility functions for erlang process dictionary.
-module(emqx_pd).

-include("types.hrl").

-export([
    get_counters/1,
    get_counter/1,
    inc_counter/2,
    reset_counter/1
]).

-compile(
    {inline, [
        get_counters/1,
        get_counter/1,
        inc_counter/2,
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
        Cnt -> Cnt
    end.

-spec inc_counter(key(), number()) -> option(number()).
inc_counter(Key, Inc) ->
    put(Key, get_counter(Key) + Inc).

-spec reset_counter(key()) -> number().
reset_counter(Key) ->
    case put(Key, 0) of
        undefined -> 0;
        Cnt -> Cnt
    end.
