%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Allocator is used to serve shared limiters.
%% Shared limiters are limiters that are consumed by multiple clients.

-module(emqx_limiter_allocator).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    start_link/2,
    update/1
]).

-type bucket() :: #{
    name := emqx_limiter:name(),
    counter := counters:counters_ref(),
    last_alloc_time := millisecond(),
    index := index(),
    correction := float()
}.

-type state() :: #{
    group := emqx_limiter:group(),
    buckets := #{emqx_limiter:name() => bucket()},
    counter := counters:counters_ref(),
    timers := #{pos_integer() => [emqx_limiter:name()]},
    burst_timers := #{pos_integer() => [emqx_limiter:name()]}
}.

-type millisecond() :: non_neg_integer().
-type index() :: pos_integer().

-define(VIA_GPROC(Id), {via, gproc, {n, l, {?MODULE, Id}}}).

%% Message types

%% NOTE
%% We need to update settings of the enclosed limiters
%% We will read the actual settings from the registry
-record(update, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(emqx_limiter:group(), [emqx_limiter:name()]) -> _.
start_link(Group, LimiterNames) when length(LimiterNames) > 0 ->
    gen_server:start_link(?VIA_GPROC(Group), ?MODULE, [Group, LimiterNames], []).

-spec update(emqx_limiter:group()) -> ok.
update(Group) ->
    gen_server:call(?VIA_GPROC(Group), #update{}).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(list()) -> {ok, state()}.
init([Group, LimiterNames]) ->
    State0 = #{
        group => Group,
        counter => counters:new(length(LimiterNames), [write_concurrency]),
        buckets => #{},
        last_alloc_time => now_ms_monotonic(),
        timers => #{},
        burst_timers => #{}
    },
    State1 = create_buckets(State0, LimiterNames),
    State2 = ensure_alloc_timers(State1),
    State3 = ensure_burst_timers(State2),
    {ok, State3}.

handle_call(#update{}, _From, State0) ->
    State1 = ensure_alloc_timers(State0),
    State2 = ensure_burst_timers(State1),
    {reply, ok, State2};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info({tick_alloc_event, Interval}, State) ->
    {noreply, handle_alloc_timer(State, Interval)};
handle_info({tick_burst_alloc_event, Interval}, State) ->
    {noreply, handle_burst_timer(State, Interval)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{group := Group, buckets := Buckets} = _State) ->
    maps:foreach(
        fun(LimiterName, _) ->
            LimiterId = {Group, LimiterName},
            emqx_limiter_bucket_registry:delete_bucket(LimiterId)
        end,
        Buckets
    ),
    ok.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

create_buckets(State, LimiterNames) ->
    {_, Buckets} = lists:foldl(
        fun(LimiterName, {Index, BucketsAcc}) ->
            Bucket = create_bucket(
                State, LimiterName, Index
            ),
            {Index + 1, BucketsAcc#{LimiterName => Bucket}}
        end,
        {1, #{}},
        LimiterNames
    ),
    State#{buckets => Buckets}.

create_bucket(#{group := Group, counter := Counter}, LimiterName, Index) ->
    LimiterId = {Group, LimiterName},
    Options = get_limiter_options(LimiterId),
    ok = set_initial_tokens(Counter, Index, Options),
    BucketRef = #{counter => Counter, index => Index},
    ?SLOG(warning, #{
        msg => "emqx_limiter_allocator_create_bucket",
        limiter_id => LimiterId,
        bucket_ref => BucketRef,
        options => Options
    }),
    ok = emqx_limiter_bucket_registry:insert_bucket(LimiterId, BucketRef),
    #{
        counter => Counter,
        index => Index,
        correction => 0,
        last_alloc_time => now_ms_monotonic()
    }.

alloc_tokens(
    %% regular or burst
    Kind,
    #{
        buckets := Buckets0,
        group := Group
    } = State,
    Names
) ->
    Buckets = alloc_buckets(Kind, Buckets0, Group, Names),
    State#{
        buckets := Buckets
    }.

alloc_buckets(Kind, Buckets, Group, Names) ->
    lists:foldl(
        fun(Name, BucketsAcc) ->
            LimiterId = {Group, Name},
            Bucket0 = maps:get(Name, BucketsAcc),
            Bucket = alloc_bucket(Kind, LimiterId, Bucket0),
            BucketsAcc#{Name => Bucket}
        end,
        Buckets,
        Names
    ).

alloc_bucket(
    regular,
    LimiterId,
    #{
        correction := Correction,
        counter := Counter,
        index := Index,
        last_alloc_time := LastTime
    } = Bucket
) ->
    Now = now_ms_monotonic(),
    Elapsed = Now - LastTime,
    Val = counters:get(Counter, Index),
    Options = get_limiter_options(LimiterId),
    case Options of
        #{capacity := infinity} ->
            Bucket;
        #{capacity := Capacity, burst_capacity := BurstCapacity} when
            Val >= Capacity + BurstCapacity
        ->
            Bucket#{last_alloc_time => Now};
        #{capacity := Capacity, burst_capacity := BurstCapacity, interval := Interval} ->
            Inc = Elapsed * Capacity / Interval + Correction,
            Inc2 = erlang:floor(Inc),
            Correction2 = Inc - Inc2,
            ?SLOG(warning, #{
                limiter_id => LimiterId,
                msg => "limiter_shared_add_tokens",
                val => Val,
                tokens => Inc2,
                options => Options
            }),
            add_tokens(Bucket, Val, Capacity + BurstCapacity, Inc2),
            Bucket#{correction := Correction2, last_alloc_time => Now}
    end;
alloc_bucket(
    burst,
    LimiterId,
    #{
        counter := Counter,
        index := Index
    } = Bucket
) ->
    Val = counters:get(Counter, Index),
    Options = get_limiter_options(LimiterId),
    case Options of
        #{capacity := infinity} ->
            Bucket;
        #{burst_capacity := 0} ->
            Bucket;
        #{capacity := Capacity, burst_capacity := BurstCapacity} ->
            ?SLOG(warning, #{
                limiter_id => LimiterId,
                msg => "limiter_shared_set_burst_capacity",
                val => Val,
                options => Options
            }),
            ok = counters:put(Counter, Index, Capacity + BurstCapacity),
            Bucket
    end.

set_initial_tokens(_Counter, _Ix, #{capacity := infinity}) ->
    ok;
set_initial_tokens(Counter, Ix, #{capacity := Capacity, burst_capacity := BurstCapacity}) ->
    counters:put(Counter, Ix, Capacity + BurstCapacity).

add_tokens(#{counter := Counter, index := Index}, Val, FullCapacity, Tokens) ->
    case Val + Tokens > FullCapacity of
        true ->
            counters:put(Counter, Index, FullCapacity);
        false ->
            counters:add(Counter, Index, Tokens)
    end.

get_limiter_options(LimiterId) ->
    emqx_limiter_registry:get_limiter_options(LimiterId).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

%% @doc Timers is a mapping of intervals into lists of limiter names.
%% Timers = #{
%%   1000 => [messages],
%%   5000 => [bytes, max_conn]
%% }
%%
%% On ensure, we only schedule timers for limiter names that are not in the timers map.
%%
%% We ignore the temorary skew that may be caused by rate updates on the fly.
%% So, we do not reschedule timers for limiter names that are already in the timers map,
%% even if the interval or capacity has changed. The allocation will be corrected in the next tick.

%% Capacity refilling

ensure_alloc_timers(#{buckets := Buckets} = State) ->
    do_ensure_alloc_timers(State, maps:keys(Buckets) -- scheduled_alloc_names(State)).

ensure_alloc_timers(State, Names) ->
    do_ensure_alloc_timers(State, Names -- scheduled_alloc_names(State)).

do_ensure_alloc_timers(State, []) ->
    State;
do_ensure_alloc_timers(#{timers := Timers0, group := Group} = State, [Name | Names]) ->
    LimiterId = {Group, Name},
    case get_limiter_options(LimiterId) of
        #{capacity := infinity} ->
            do_ensure_alloc_timers(State, Names);
        #{interval := Interval} ->
            Timers =
                case Timers0 of
                    #{Interval := IntervalNames0} ->
                        IntervalNames = [Name | IntervalNames0],
                        Timers0#{Interval => IntervalNames};
                    _ ->
                        _ = erlang:send_after(Interval, self(), {tick_alloc_event, Interval}),
                        Timers0#{Interval => [Name]}
                end,
            do_ensure_alloc_timers(State#{timers => Timers}, Names)
    end.

handle_alloc_timer(#{timers := Timers0} = State0, Interval) ->
    {Names, Timers} = maps:take(Interval, Timers0),
    State1 = State0#{timers => Timers},
    State2 = alloc_tokens(regular, State1, Names),
    ensure_alloc_timers(State2, Names).

scheduled_alloc_names(#{timers := Timers}) ->
    lists:append(maps:values(Timers)).

%% Burst capacity refilling

ensure_burst_timers(#{buckets := Buckets} = State) ->
    do_ensure_burst_timers(State, maps:keys(Buckets) -- scheduled_burst_names(State)).

ensure_burst_timers(State, Names) ->
    do_ensure_burst_timers(State, Names -- scheduled_burst_names(State)).

do_ensure_burst_timers(State, []) ->
    State;
do_ensure_burst_timers(#{burst_timers := BurstTimers0, group := Group} = State, [Name | Names]) ->
    LimiterId = {Group, Name},
    case get_limiter_options(LimiterId) of
        #{capacity := infinity} ->
            do_ensure_burst_timers(State, Names);
        #{burst_capacity := 0} ->
            do_ensure_burst_timers(State, Names);
        #{burst_capacity := BurstCapacity, burst_interval := Interval} when BurstCapacity > 0 ->
            BurstTimers =
                case BurstTimers0 of
                    #{Interval := IntervalNames0} ->
                        IntervalNames = [Name | IntervalNames0],
                        BurstTimers0#{Interval => IntervalNames};
                    _ ->
                        _ = erlang:send_after(Interval, self(), {tick_burst_alloc_event, Interval}),
                        BurstTimers0#{Interval => [Name]}
                end,
            do_ensure_burst_timers(State#{burst_timers => BurstTimers}, Names)
    end.

handle_burst_timer(#{burst_timers := BurstTimers0} = State0, Interval) ->
    {Names, BurstTimers} = maps:take(Interval, BurstTimers0),
    State1 = State0#{burst_timers => BurstTimers},
    State2 = alloc_tokens(burst, State1, Names),
    ensure_burst_timers(State2, Names).

scheduled_burst_names(#{burst_timers := BurstTimers}) ->
    lists:append(maps:values(BurstTimers)).
