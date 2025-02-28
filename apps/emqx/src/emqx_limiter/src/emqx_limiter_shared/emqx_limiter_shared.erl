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

%% @doc This module implements the shared token bucket limiter.
%%
%% Shared limiter is a limiter that is shared between processes.
%% I.e. several processes consume tokens concurrently from the same bucket.
%%
%% The shared limiter's capacity is modelled as time interval spanning into the past.
%% When we want to consume Amount tokens, we shrink the interval by the time
%% corresponding to Amount tokens, i.e. having rate = N tokens / T seconds,
%% we shrink the interval by T * Amount / N seconds.
%%
%% The bucket is exhausted when the interval shrinks to the current time.
%% As the current time extends forward, the time interval naturally extends
%% modelling the bucket being refilled.

-module(emqx_limiter_shared).

-behaviour(emqx_limiter_client).
-behaviour(emqx_limiter).

-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% emqx_limiter callbacks
-export([
    create_group/2,
    update_group/2,
    delete_group/1,
    connect/1
]).

%% emqx_limiter_client callbacks
-export([
    try_consume/2,
    put_back/2
]).

-export([
    inspect/1
]).

-type bucket_ref() :: #{
    mini_tokens_cref := counters:counters_ref(),
    mini_tokens_index := pos_integer(),
    last_time_aref := atomics:atomics_ref(),
    last_time_index := pos_integer(),
    last_burst_time_index := pos_integer()
}.

-type client_state() :: #{
    limiter_id := emqx_limiter:id(),
    bucket_ref := bucket_ref(),
    mode := mode(),
    options := emqx_limiter:options(),
    last_burst_time := microsecond()
}.

-type microsecond() :: non_neg_integer().
-export_type([bucket_ref/0, client_state/0]).

%% For tight limits, <1000/s
-record(token_mode, {
    us_per_token :: pos_integer(),
    max_time_us :: pos_integer()
}).

%% For loose limits, >=1000/s, e.g. data rate limits, 10MB/1m
%%
%% With tokens that cost too little, we extend the time interval by
%% several tokens at once, putting unused tokens into a separate counter.
-record(mini_token_mode, {
    mini_token_per_ms :: pos_integer(),
    max_time_us :: pos_integer()
}).

-type mode() :: #token_mode{} | #mini_token_mode{}.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create_group(emqx_limiter:group(), [
    {emqx_limiter:name(), emqx_limiter:options()}
]) -> ok | {error, term()}.
create_group(Group, LimiterConfigs) when length(LimiterConfigs) > 0 ->
    Size = length(LimiterConfigs),
    MiniTokensCRef = counters:new(Size, [write_concurrency]),
    LastTimeARef = atomics:new(Size * 2, []),
    NowUs = now_us_monotonic(),
    Buckets = make_buckets(LimiterConfigs, MiniTokensCRef, LastTimeARef, NowUs),
    ok = emqx_limiter_bucket_registry:insert_buckets(Group, Buckets).

-spec delete_group(emqx_limiter:group()) -> ok | {error, term()}.
delete_group(Group) ->
    ok = emqx_limiter_bucket_registry:delete_buckets(Group).

-spec update_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok.
update_group(_Group, _LimiterConfigs) ->
    ok.

-spec connect(emqx_limiter:id()) -> emqx_limiter_client:t().
connect({Group, Name}) ->
    case emqx_limiter_bucket_registry:find_bucket({Group, Name}) of
        undefined ->
            error({bucket_not_found, {Group, Name}});
        #{last_time_aref := LastBurstTimeARef, last_burst_time_index := LastBurstTimeIndex} =
                BucketRef ->
            Options = emqx_limiter_registry:get_limiter_options({Group, Name}),
            emqx_limiter_client:new(
                ?MODULE,
                _State = #{
                    limiter_id => {Group, Name},
                    bucket_ref => BucketRef,
                    last_burst_time => atomics:get(LastBurstTimeARef, LastBurstTimeIndex),
                    mode => calc_mode(Options),
                    options => Options
                }
            )
    end.

%%--------------------------------------------------------------------
%% emqx_limiter_client callbacks
%%--------------------------------------------------------------------

-spec try_consume(client_state(), non_neg_integer()) -> boolean().
try_consume(#{limiter_id := LimiterId} = State, Amount) ->
    Options = emqx_limiter_registry:get_limiter_options(LimiterId),
    case Options of
        #{capacity := infinity} ->
            true;
        _ ->
            {Result, State1} = try_consume(State, Options, Amount, now_us_monotonic()),
            ?tp(limiter_shared_try_consume, #{
                limiter_id => LimiterId,
                amount => Amount,
                result => Result
            }),
            {Result, State1}
    end.

-spec put_back(client_state(), non_neg_integer()) -> client_state().
put_back(#{bucket_ref := BucketRef, mode := Mode} = State, Amount) ->
    case Mode of
        #token_mode{us_per_token = UsPerToken} ->
            #{last_time_aref := LastTimeARef, last_time_index := LastTimeIndex} = BucketRef,
            ok = atomics:sub(LastTimeARef, LastTimeIndex, UsPerToken * Amount);
        #mini_token_mode{} ->
            #{mini_tokens_cref := MiniTokensCRef, mini_tokens_index := MiniTokensIndex} = BucketRef,
            AmountMini = Amount * 1000,
            ok = counters:add(MiniTokensCRef, MiniTokensIndex, AmountMini)
    end,
    State.

-spec inspect(client_state()) -> map().
inspect(#{bucket_ref := BucketRef, mode := Mode, options := Options} = _State) ->
    #{last_time_aref := LastTimeARef, last_time_index := LastTimeIndex} = BucketRef,
    LastTimeUs = atomics:get(LastTimeARef, LastTimeIndex),
    #{mini_tokens_cref := MiniTokensCRef, mini_tokens_index := MiniTokensIndex} = BucketRef,
    MiniTokens = counters:get(MiniTokensCRef, MiniTokensIndex),
    #{capacity := Capacity, interval := IntervalMs} = Options,
    TimeLeftUs = now_us_monotonic() - LastTimeUs,
    #{
        mode => Mode,
        time_left_us => TimeLeftUs,
        mini_tokens => MiniTokens,
        tokens_left => ((TimeLeftUs * Capacity) div IntervalMs) div 1000
    }.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

make_buckets(Names, MiniTokensCRef, LastTimeARef, NowUs) ->
    make_buckets(Names, MiniTokensCRef, LastTimeARef, NowUs, 1, []).

make_buckets([], _MiniTokensCRef, _LastTimeARef, _NowUs, _Index, Res) ->
    lists:reverse(Res);
make_buckets(
    [{Name, Options} | Names], MiniTokensCRef, LastTimeARef, NowUs, Index, Res
) ->
    BucketRef = #{
        mini_tokens_cref => MiniTokensCRef,
        mini_tokens_index => Index,
        last_time_aref => LastTimeARef,
        last_time_index => Index,
        last_burst_time_index => Index + 1
    },
    Mode = calc_mode(Options),
    ok = apply_burst(Mode, Options, BucketRef, NowUs),
    make_buckets(Names, MiniTokensCRef, LastTimeARef, NowUs, Index + 2, [
        {Name, BucketRef} | Res
    ]).

try_consume(State0, Options, Amount, NowUs) ->
    {Mode, State1} = mode(State0, Options),
    try_consume(State1, Mode, Options, Amount, NowUs).

try_consume(
    #{bucket_ref := BucketRef} = State,
    #token_mode{us_per_token = UsPerToken, max_time_us = MaxTimeUs} = Mode,
    Options,
    Amount,
    NowUs
) ->
    #{last_time_aref := LastTimeARef, last_time_index := LastTimeIndex} = BucketRef,
    LastTimeUs = atomics:get(LastTimeARef, LastTimeIndex),
    UsRequired = UsPerToken * Amount,
    case NowUs - LastTimeUs > UsRequired of
        true ->
            LastTimeUsNew = max(UsRequired + LastTimeUs, UsRequired + NowUs - MaxTimeUs),
            case atomics:compare_exchange(LastTimeARef, LastTimeIndex, LastTimeUs, LastTimeUsNew) of
                ok ->
                    {true, State};
                _ ->
                    try_consume(State, Mode, Options, Amount, now_us_monotonic())
            end;
        false ->
            try_consume_burst(State, Mode, Options, Amount, NowUs)
    end;
try_consume(
    #{bucket_ref := BucketRef} = State,
    #mini_token_mode{mini_token_per_ms = MiniTokenPerMs, max_time_us = MaxTimeUs} = Mode,
    Options,
    Amount,
    NowUs
) ->
    #{mini_tokens_cref := MiniTokensCRef, mini_tokens_index := MiniTokensIndex} = BucketRef,
    AmountMini = Amount * 1000,
    case counters:get(MiniTokensCRef, MiniTokensIndex) - AmountMini > 0 of
        true ->
            ok = counters:sub(MiniTokensCRef, MiniTokensIndex, AmountMini),
            {true, State};
        false ->
            {UsRequired, LeftOver} = amount_to_required_time_and_leftover(
                MiniTokenPerMs, AmountMini
            ),
            #{last_time_aref := LastTimeARef, last_time_index := LastTimeIndex} = BucketRef,
            LastTimeUs = atomics:get(LastTimeARef, LastTimeIndex),
            case NowUs - LastTimeUs > UsRequired of
                true ->
                    LastTimeUsNew = max(UsRequired + LastTimeUs, UsRequired + NowUs - MaxTimeUs),
                    case
                        atomics:compare_exchange(
                            LastTimeARef, LastTimeIndex, LastTimeUs, LastTimeUsNew
                        )
                    of
                        ok ->
                            ok = counters:add(MiniTokensCRef, MiniTokensIndex, LeftOver),
                            {true, State};
                        _ ->
                            try_consume(State, Mode, Options, Amount, now_us_monotonic())
                    end;
                false ->
                    try_consume_burst(State, Mode, Options, Amount, NowUs)
            end
    end.

try_consume_burst(State, _Mode, #{burst_capacity := 0}, _Amount, _NowUs) ->
    {false, State};
try_consume_burst(
    #{last_burst_time := LastBurstTimeUs} = State,
    _Mode,
    #{burst_interval := BurstIntervalMs} = _Options,
    _Amount,
    NowUs
) when NowUs < LastBurstTimeUs + BurstIntervalMs * 1000 ->
    {false, State};
try_consume_burst(
    #{bucket_ref := BucketRef} = State,
    Mode,
    #{burst_interval := BurstIntervalMs} = Options,
    Amount,
    NowUs
) ->
    #{last_time_aref := LastBurstTimeARef, last_burst_time_index := LastBurstTimeIndex} =
        BucketRef,
    LastBurstTimeUs = atomics:get(LastBurstTimeARef, LastBurstTimeIndex),
    case NowUs < LastBurstTimeUs + BurstIntervalMs * 1000 of
        true ->
            {false, State#{last_burst_time := LastBurstTimeUs}};
        false ->
            apply_burst_and_consume(State, Mode, Options, Amount, NowUs)
    end.

apply_burst_and_consume(#{bucket_ref := BucketRef} = State, Mode, Options, Amount, NowUs) ->
    ok = apply_burst(Mode, Options, BucketRef, NowUs),
    try_consume(State#{last_burst_time := NowUs}, Mode, Options, Amount, NowUs).

apply_burst(_Mode, #{capacity := infinity}, _BucketRef, _NowUs) ->
    ok;
apply_burst(Mode, _Options, BucketRef, NowUs) ->
    #{
        last_time_aref := LastBurstTimeARef,
        last_burst_time_index := LastBurstTimeIndex,
        last_time_aref := LastTimeARef,
        last_time_index := LastTimeIndex
    } = BucketRef,
    LastTimeUsNew =
        case Mode of
            #token_mode{max_time_us = MaxTimeUs} ->
                NowUs - MaxTimeUs;
            #mini_token_mode{max_time_us = MaxTimeUs} ->
                NowUs - MaxTimeUs
        end,
    ok = atomics:put(LastTimeARef, LastTimeIndex, LastTimeUsNew),
    ok = atomics:put(LastBurstTimeARef, LastBurstTimeIndex, NowUs).

now_us_monotonic() ->
    erlang:monotonic_time(microsecond).

mode(#{options := Options, mode := Mode} = State, Options) ->
    {Mode, State};
mode(State, NewOptions) ->
    Mode = calc_mode(NewOptions),
    {Mode, State#{mode := Mode}}.

calc_mode(#{capacity := infinity}) ->
    #token_mode{us_per_token = 1000, max_time_us = 1000};
calc_mode(#{capacity := Capacity, interval := IntervalMs, burst_capacity := BurstCapacity}) ->
    FullCapacity = Capacity + BurstCapacity,
    MaxTimeUs = (1000 * IntervalMs * FullCapacity) div Capacity,
    case Capacity < IntervalMs of
        true ->
            UsPerToken = (1000 * IntervalMs) div Capacity,
            #token_mode{
                us_per_token = UsPerToken,
                max_time_us = MaxTimeUs
            };
        false ->
            #mini_token_mode{
                mini_token_per_ms = (1000 * Capacity) div IntervalMs,
                max_time_us = MaxTimeUs
            }
    end.

amount_to_required_time_and_leftover(MiniTokensPerMs, AmountMini) ->
    case AmountMini rem MiniTokensPerMs of
        0 ->
            MsRequired = AmountMini div MiniTokensPerMs,
            LeftOver = 0;
        Rem ->
            MsRequired = AmountMini div MiniTokensPerMs + 1,
            LeftOver = MiniTokensPerMs - Rem
    end,
    {MsRequired * 1000, LeftOver}.
