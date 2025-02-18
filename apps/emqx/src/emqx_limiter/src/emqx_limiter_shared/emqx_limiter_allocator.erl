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
    free_indices := [index()]
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
        timers => #{}
    },
    State1 = create_buckets(State0, LimiterNames),
    State2 = ensure_timers(State1),
    {ok, State2}.

handle_call(#update{}, _From, State) ->
    {reply, ok, ensure_timers(State)};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info({tick_alloc_event, Interval}, State) ->
    {noreply, handle_timer(State, Interval)};
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
    _Options = #{capacity := Capacity} = get_limiter_options(LimiterId),
    ok = set_initial_tokens(Counter, Index, Capacity),
    BucketRef = #{counter => Counter, index => Index},
    ?SLOG(warning, #{
        msg => "emqx_limiter_allocator_create_bucket",
        limiter_id => LimiterId,
        bucket_ref => BucketRef,
        options => _Options
    }),
    ok = emqx_limiter_bucket_registry:insert_bucket(LimiterId, BucketRef),
    #{
        counter => Counter,
        index => Index,
        correction => 0,
        last_alloc_time => now_ms_monotonic()
    }.

alloc_tokens(
    #{
        buckets := Buckets0,
        group := Group
    } = State,
    Names
) ->
    Buckets = alloc_buckets(Buckets0, Group, Names),
    State#{
        buckets := Buckets
    }.

alloc_buckets(Buckets, Group, Names) ->
    lists:foldl(
        fun(Name, BucketsAcc) ->
            LimiterId = {Group, Name},
            Bucket0 = maps:get(Name, BucketsAcc),
            Bucket = alloc_bucket(LimiterId, Bucket0),
            BucketsAcc#{Name => Bucket}
        end,
        Buckets,
        Names
    ).

alloc_bucket(
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
        #{capacity := Capacity} when Val >= Capacity ->
            Bucket#{last_alloc_time => Now};
        #{capacity := Capacity, interval := Interval} ->
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
            add_tokens(Bucket, Val, Capacity, Inc2),
            Bucket#{correction := Correction2, last_alloc_time => Now}
    end.

set_initial_tokens(_Counter, _Ix, infinity) ->
    ok;
set_initial_tokens(Counter, Ix, Capacity) ->
    counters:put(Counter, Ix, Capacity).

add_tokens(#{counter := Counter, index := Index}, Val, Capacity, Tokens) ->
    case Val + Tokens > Capacity of
        true ->
            counters:put(Counter, Index, Capacity);
        false ->
            counters:add(Counter, Index, Tokens)
    end.

get_limiter_options(LimiterId) ->
    case emqx_limiter_registry:get_limiter_options(LimiterId) of
        undefined ->
            error({limiter_not_found, LimiterId});
        Options ->
            Options
    end.

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
%%
ensure_timers(#{buckets := Buckets} = State) ->
    ensure_timers(State, maps:keys(Buckets)).

ensure_timers(State, []) ->
    State;
ensure_timers(#{timers := Timers0, group := Group} = State, [Name | Names]) ->
    LimiterId = {Group, Name},
    case get_limiter_options(LimiterId) of
        #{capacity := infinity} ->
            ensure_timers(State, Names);
        #{interval := Interval} ->
            case Timers0 of
                #{Interval := IntervalNames0} ->
                    IntervalNames = [Name | IntervalNames0],
                    Timers = Timers0#{Interval => IntervalNames};
                _ ->
                    _ = erlang:send_after(Interval, self(), {tick_alloc_event, Interval}),
                    Timers = Timers0#{Interval => [Name]}
            end,
            ensure_timers(State#{timers => Timers}, Names)
    end.

handle_timer(#{timers := Timers0} = State0, Interval) ->
    {Names, Timers} = maps:take(Interval, Timers0),
    State1 = State0#{timers => Timers},
    State2 = alloc_tokens(State1, Names),
    ensure_timers(State2, Names).
