%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements the private limiter.
%%
%% Exclusive limiter is a limiter that is not shared between different processes.
%% I.e. different processes do not consume tokens concurrently.
%%
%% This is a more simple version of a limiter because its own toket bucket
%% is refilled algorithmically, without the help of the external emqx_limiter_allocator.

-module(emqx_limiter_exclusive).

-behaviour(emqx_limiter_client).
-behaviour(emqx_limiter).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% emqx_limiter callbacks
-export([
    create_group/2,
    delete_group/1,
    update_group/2,
    connect/1
]).

%% emqx_limiter_client API
-export([
    try_consume/2,
    put_back/2
]).

-type millisecond() :: integer().

-type state() :: #{
    limiter_id := emqx_limiter:id(),
    tokens := number(),
    last_time := millisecond(),
    last_burst_time := millisecond()
}.

-type reason() :: emqx_limiter_client:reason().

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

%% NOTE
%% Group operations are no-ops because the the buckets are on the client side.
%% The limiter's state is just the settings, we do not need to update anything here.

-spec create_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) -> ok.
create_group(_Group, _LimiterConfigs) ->
    ok.

-spec delete_group(emqx_limiter:group()) -> ok.
delete_group(_Group) ->
    ok.

-spec update_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok.
update_group(_Group, _LimiterConfigs) ->
    ok.

-spec connect(emqx_limiter:id()) -> emqx_limiter_client:t().
connect({_Group, _Name} = LimiterId) ->
    Options = emqx_limiter_registry:get_limiter_options(LimiterId),
    Now = now_ms_monotonic(),
    State = #{
        limiter_id => LimiterId,
        tokens => 0,
        burst_tokens => 0,
        last_time => initial_time(Options, Now),
        last_burst_time => initial_burst_time(Options, Now)
    },
    emqx_limiter_client:new(?MODULE, State).

%%--------------------------------------------------------------------
%% emqx_limiter_client API
%%--------------------------------------------------------------------

-spec try_consume(state(), number()) -> {true, state()} | {false, state(), reason()}.
try_consume(#{limiter_id := LimiterId} = State0, Amount) ->
    LimiterOptions = emqx_limiter_registry:get_limiter_options(LimiterId),
    Result =
        case try_consume(State0, Amount, LimiterOptions) of
            {true = Success, State1} ->
                {true, State1};
            {false = Success, State1} ->
                {false, State1, {failed_to_consume_from_limiter, LimiterId}}
        end,
    ?tp(limiter_exclusive_try_consume, #{
        limiter_id => LimiterId,
        amount => Amount,
        success => Success
    }),
    Result.

-spec put_back(state(), number()) -> state().
put_back(#{tokens := Tokens} = State, Amount) ->
    State#{tokens := Tokens + Amount}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

try_consume(State, _Amount, #{capacity := infinity}) ->
    {true, State};
try_consume(#{tokens := Tokens, burst_tokens := BurstTokens} = State, Amount, _LimiterOptions) when
    Amount =< Tokens + BurstTokens
->
    {true, consume_existing(State, Amount)};
try_consume(State0, Amount, LimiterOptions) ->
    Now = now_ms_monotonic(),
    case try_consume_regular(State0, LimiterOptions, Amount, Now) of
        {true, State1} ->
            {true, State1};
        {false, State1} ->
            try_consume_burst(State1, LimiterOptions, Amount, Now)
    end.

try_consume_regular(#{last_time := LastTime} = State0, #{interval := Interval}, _Amount, Now) when
    Now < LastTime + Interval
->
    {false, State0};
try_consume_regular(
    #{last_time := LastTime, tokens := Tokens0, burst_tokens := BurstTokens} = State0,
    #{capacity := Capacity, interval := Interval},
    Amount,
    Now
) ->
    Inc = Capacity * (Now - LastTime) / Interval,
    Tokens = erlang:min(Capacity, Tokens0 + Inc),
    State1 = State0#{last_time := Now, tokens := Tokens},
    case Tokens + BurstTokens >= Amount of
        true ->
            {true, consume_existing(State1, Amount)};
        false ->
            {false, State1}
    end.

try_consume_burst(State0, #{burst_capacity := 0}, _Amount, _Now) ->
    {false, State0};
try_consume_burst(
    #{last_burst_time := LastBurstTime} = State0, #{burst_interval := BurstInterval}, _Amount, Now
) when Now < LastBurstTime + BurstInterval ->
    {false, State0};
try_consume_burst(#{tokens := Tokens} = State0, #{burst_capacity := BurstCapacity}, Amount, Now) ->
    State1 = State0#{last_burst_time := Now, burst_tokens := BurstCapacity},
    case Tokens + BurstCapacity >= Amount of
        true ->
            {true, consume_existing(State1, Amount)};
        false ->
            {false, State1}
    end.

consume_existing(#{tokens := Tokens, burst_tokens := BurstTokens} = State, Amount) when
    Amount =< Tokens + BurstTokens
->
    ConsumedBurstTokens = erlang:min(Amount, BurstTokens),
    ConsumedRegularTokens = Amount - ConsumedBurstTokens,
    State#{
        tokens := Tokens - ConsumedRegularTokens, burst_tokens := BurstTokens - ConsumedBurstTokens
    }.

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

initial_time(#{capacity := infinity}, Now) ->
    Now;
initial_time(#{interval := Interval}, Now) ->
    Now - Interval - 1.

initial_burst_time(#{capacity := infinity}, Now) ->
    Now;
initial_burst_time(#{burst_capacity := 0}, Now) ->
    Now;
initial_burst_time(#{burst_interval := BurstInterval}, Now) ->
    Now - BurstInterval - 1.
