%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements the shared token bucket limiter.
%%
%% Shared limiter is a limiter that is shared between processes.
%% I.e. several processes consume tokens concurrently from the same bucket.
%%
%% The shared limiter's regular capacity is modelled as time interval spanning into the past.
%% When we want to consume Amount tokens, we shrink the interval by the time
%% corresponding to Amount tokens, i.e. having rate = N tokens / T seconds,
%% we shrink the interval by T * Amount / N seconds.
%%
%% The bucket is exhausted when the interval shrinks to the current time.
%% As the current time extends forward, the time interval naturally extends
%% modelling the bucket being refilled.
%%
%% The shared limiter's burst capacity is managed more losely and
%% modelled as a simple separate bucket that is refilled when the regular bucket is exhausted.

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

-record(atomic_value, {
    aref :: atomics:atomics_ref(),
    index :: pos_integer()
}).

-type atomic_value() :: #atomic_value{}.

-type bucket_ref() :: #{
    mini_tokens := atomic_value(),
    last_time := atomic_value(),
    burst_tokens := atomic_value(),
    last_burst_time := atomic_value()
}.

-type client_state() :: #{
    limiter_id := emqx_limiter:id(),
    bucket_ref := bucket_ref(),
    mode := mode(),
    options := emqx_limiter:options()
}.

-type reason() :: emqx_limiter_client:reason().

-export_type([bucket_ref/0, client_state/0]).

%% For tight limits, <1000/s
-record(token_mode, {
    us_per_token :: pos_integer(),
    %% The maximum time models the maximum bucket size.
    max_time_us :: pos_integer()
}).

%% For loose limits, >=1000/s, e.g. data rate limits, 10MB/1m
%%
%% With tokens that cost too little, we extend the time interval by
%% several tokens at once, putting unused tokens into a separate counter.
-record(mini_token_mode, {
    mini_tokens_per_ms :: pos_integer(),
    %% The maximum time models the maximum bucket size.
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
    %% Factor 3 is because we have 4 atomics per limiter:
    %% mini_tokens
    %% last_time
    %% burst_tokens
    %% last_burst_time
    ARef = atomics:new(Size * 4, []),
    NowUs = now_us_monotonic(),
    Buckets = make_buckets(LimiterConfigs, ARef, NowUs),
    ok = emqx_limiter_bucket_registry:insert_buckets(Group, Buckets).

-spec delete_group(emqx_limiter:group()) -> ok | {error, term()}.
delete_group(Group) ->
    ok = emqx_limiter_bucket_registry:delete_buckets(Group).

-spec update_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok.
update_group(_Group, _LimiterConfigs) ->
    ok.

-spec connect(emqx_limiter:id()) -> emqx_limiter_client:t().
connect(LimiterId) ->
    case emqx_limiter_bucket_registry:find_bucket(LimiterId) of
        undefined ->
            error({bucket_not_found, LimiterId});
        BucketRef ->
            Options = emqx_limiter_registry:get_limiter_options(LimiterId),
            emqx_limiter_client:new(
                ?MODULE,
                _State = #{
                    limiter_id => LimiterId,
                    bucket_ref => BucketRef,
                    mode => calc_mode(Options),
                    options => Options
                }
            )
    end.

%%--------------------------------------------------------------------
%% emqx_limiter_client callbacks
%%--------------------------------------------------------------------

-spec try_consume(client_state(), non_neg_integer()) ->
    true | {true, client_state()} | {false, client_state(), reason()}.
try_consume(#{limiter_id := LimiterId} = State, Amount) ->
    Options = emqx_limiter_registry:get_limiter_options(LimiterId),
    case Options of
        #{capacity := infinity} ->
            true;
        _ ->
            Result =
                case try_consume(State, Options, Amount) of
                    {true = Success, State1} ->
                        {true, State1};
                    {false = Success, State1} ->
                        {false, State1, {failed_to_consume_from_limiter, LimiterId}}
                end,
            ?tp(limiter_shared_try_consume, #{
                limiter_id => LimiterId,
                amount => Amount,
                success => Success
            }),
            Result
    end.

-spec put_back(client_state(), non_neg_integer()) -> client_state().
put_back(#{bucket_ref := BucketRef, mode := Mode} = State, Amount) ->
    case Mode of
        #token_mode{us_per_token = UsPerToken} ->
            #{last_time := LastTime} = BucketRef,
            ok = atomic_sub(LastTime, UsPerToken * Amount);
        #mini_token_mode{} ->
            #{mini_tokens := MiniTokens} = BucketRef,
            AmountMini = Amount * 1000,
            ok = atomic_add(MiniTokens, AmountMini)
    end,
    State.

-spec inspect(client_state()) -> map().
inspect(#{bucket_ref := BucketRef, mode := Mode, options := Options} = _State) ->
    #{last_time := LastTime, mini_tokens := MiniTokens} = BucketRef,
    LastTimeUs = atomic_get(LastTime),
    #{capacity := Capacity, interval := IntervalMs} = Options,
    TimeLeftUs = now_us_monotonic() - LastTimeUs,
    #{
        mode => Mode,
        time_left_us => TimeLeftUs,
        mini_tokens => atomic_get(MiniTokens),
        tokens_left => ((TimeLeftUs * Capacity) div IntervalMs) div 1000
    }.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

make_buckets(Names, ARef, NowUs) ->
    make_buckets(Names, ARef, NowUs, 1, []).

make_buckets([], _ARef, _NowUs, _Index, Res) ->
    lists:reverse(Res);
make_buckets(
    [{Name, Options} | Names], ARef, NowUs, Index, Res
) ->
    BucketRef = #{
        mini_tokens => #atomic_value{aref = ARef, index = 4 * Index - 3},
        last_time => #atomic_value{aref = ARef, index = 4 * Index - 2},
        last_burst_time => #atomic_value{aref = ARef, index = 4 * Index - 1},
        burst_tokens => #atomic_value{aref = ARef, index = 4 * Index}
    },
    Mode = calc_mode(Options),
    ok = init_last_time(BucketRef, Mode, Options, NowUs),
    ok = apply_burst(BucketRef, Options, NowUs),
    make_buckets(Names, ARef, NowUs, Index + 1, [
        {Name, BucketRef} | Res
    ]).

try_consume(State0, Options, Amount) ->
    {Mode, State1} = mode(State0, Options),
    {try_consume(State1, Mode, Options, Amount, now_us_monotonic()), State1}.

try_consume(State, Mode, #{burst_capacity := 0} = Options, Amount, NowUs) ->
    case try_consume_regular(State, Mode, Options, Amount, NowUs) of
        ok ->
            true;
        failed ->
            false
    end;
try_consume(State, Mode, Options, Amount, NowUs) ->
    case try_consume_accumulated_burst(State, Amount) of
        ok ->
            true;
        {failed, LastBurstTimeUs} ->
            case try_consume_regular(State, Mode, Options, Amount, NowUs) of
                ok ->
                    true;
                failed ->
                    case try_burst(State, Options, LastBurstTimeUs, NowUs) of
                        ok ->
                            try_consume(State, Mode, Options, Amount, now_us_monotonic());
                        failed ->
                            false
                    end
            end
    end.

try_consume_accumulated_burst(#{bucket_ref := BucketRef}, Amount) ->
    #{
        burst_tokens := BurstTokens,
        last_burst_time := LastBurstTime
    } = BucketRef,
    %% The order of getting LastBurstTime and checking tokens is important.
    %% If we check tokens first, we may fail while having right to burst:
    %% * we see no tokens
    %% * another process concurrently applies burst
    %% * we check LastBurstTime and see the new value
    %% * conclude that the burst was already done recently and fail
    LastBurstTimeUs = atomic_get(LastBurstTime),
    case atomic_sub_get(BurstTokens, Amount) >= 0 of
        true ->
            ok;
        false ->
            {failed, LastBurstTimeUs}
    end.

-doc """
Try to consume regular tokens when there are no accumulated burst tokens.
""".
try_consume_regular(
    #{bucket_ref := BucketRef} = State,
    #token_mode{us_per_token = UsPerToken, max_time_us = MaxTimeUs} = Mode,
    Options,
    Amount,
    NowUs
) ->
    UsRequired = UsPerToken * Amount,
    %% To succeed, at least UsRequired microseconds must pass since the last time stored in the bucket.
    case advance_last_time(BucketRef, UsRequired, NowUs, MaxTimeUs) of
        ok ->
            ok;
        retry ->
            try_consume_regular(State, Mode, Options, Amount, now_us_monotonic());
        failed ->
            failed
    end;
try_consume_regular(
    #{bucket_ref := BucketRef} = State,
    #mini_token_mode{mini_tokens_per_ms = MiniTokensPerMs, max_time_us = MaxTimeUs} = Mode,
    Options,
    Amount,
    NowUs
) ->
    #{mini_tokens := MiniTokens} = BucketRef,
    AmountMini = Amount * 1000,
    %% First try to consume the tokens from the leftovers
    %% If there are not enough, we calculate the required time to advance the last_time
    case atomic_get(MiniTokens) - AmountMini >= 0 of
        true ->
            ok = atomic_sub(MiniTokens, AmountMini);
        false ->
            %% In mini-token mode, we the granularity of tokens consumpton is > 1,
            %% so we may have left-over tokens
            {UsRequired, LeftOver} = amount_to_required_time_and_leftover(
                MiniTokensPerMs, AmountMini
            ),
            %% To succeed, at least UsRequired microseconds must pass since the last time stored in the bucket.
            case advance_last_time(BucketRef, UsRequired, NowUs, MaxTimeUs) of
                ok ->
                    ok = atomic_add(MiniTokens, LeftOver);
                retry ->
                    try_consume_regular(State, Mode, Options, Amount, now_us_monotonic());
                failed ->
                    failed
            end
    end.

try_burst(
    #{bucket_ref := BucketRef} = _State,
    #{burst_interval := BurstIntervalMs} = Options,
    LastBurstTimeUs,
    NowUs
) ->
    case NowUs < LastBurstTimeUs + BurstIntervalMs * 1000 of
        true ->
            %% Some other process has granted burst tokens recently, so we can't grant any more.
            failed;
        false ->
            %% We can grant burst tokens, so do it and consume the tokens again.
            ok = apply_burst(BucketRef, Options, NowUs)
    end.

apply_burst(BucketRef, #{burst_capacity := BurstCapacity} = _Options, NowUs) ->
    #{
        last_burst_time := LastBurstTime,
        burst_tokens := BurstTokens
    } = BucketRef,
    %% Here we don't care that the read and write are not atomic.
    %% The worst case is that several processes see the same old burst time
    %% and concurrently apply the burst, while some other processes will consume some (free) tokens
    %% between the applications.
    %%
    %% We allow this, because burst should be configured to happen quite rarely and provide large
    %% amount of tokens. So some free tokens are not a deal.
    %% For very strict limits, one should operate with regular capacity only.
    %%
    %% The order is important. If we put LastBurstTime first, then another process may
    %% * see updated last burst time
    %% * see still not updated burst tokens
    %% * fail
    %% while having a right for a burst.
    ok = atomic_put(BurstTokens, BurstCapacity),
    ok = atomic_put(LastBurstTime, NowUs);
apply_burst(BucketRef, #{} = _Options, NowUs) ->
    #{last_burst_time := LastBurstTime} = BucketRef,
    ok = atomic_put(LastBurstTime, NowUs).

advance_last_time(BucketRef, UsRequired, NowUs, MaxTimeUs) ->
    #{last_time := LastTime} = BucketRef,
    LastTimeUs = atomic_get(LastTime),
    case NowUs - LastTimeUs >= UsRequired of
        true ->
            %% There is enough capacity to advance the LastTime by UsRequired.
            %% We try to do it atomically.
            %% If LastTimeUs is very old (e.g. limiter was not used for a long time),
            %% we take NowUs - MaxTimeUs instead of LastTimeUs to limit the bucket capacity.
            LastTimeUsNew = UsRequired + max(LastTimeUs, NowUs - MaxTimeUs),
            case atomic_compare_exchange(LastTime, LastTimeUs, LastTimeUsNew) of
                ok ->
                    ok;
                _ ->
                    retry
            end;
        false ->
            %% Not enough time has passed to advance the LastTime by UsRequired
            %% meaning that not enough tokens have been generated.
            failed
    end.

init_last_time(
    #{last_time := LastTime} = _BucketRef,
    #token_mode{max_time_us = MaxTimeUs} = _Mode,
    #{} = _Options,
    NowUs
) ->
    ok = atomic_put(LastTime, NowUs - MaxTimeUs);
init_last_time(
    #{last_time := LastTime} = _BucketRef,
    #mini_token_mode{max_time_us = MaxTimeUs} = _Mode,
    #{} = _Options,
    NowUs
) ->
    ok = atomic_put(LastTime, NowUs - MaxTimeUs).

now_us_monotonic() ->
    erlang:monotonic_time(microsecond).

%% Do not re-calculate the mode if Options did not change
mode(#{options := Options, mode := Mode} = State, Options) ->
    {Mode, State};
mode(State, NewOptions) ->
    Mode = calc_mode(NewOptions),
    {Mode, State#{mode := Mode}}.

calc_mode(#{capacity := infinity}) ->
    #token_mode{us_per_token = 1, max_time_us = 1};
calc_mode(#{capacity := Capacity, interval := IntervalMs}) ->
    %% MaxTimeUs models the maximum bucket capacity (see module description).
    MaxTimeUs = 1000 * IntervalMs,
    case Capacity < IntervalMs of
        true ->
            UsPerToken = (1000 * IntervalMs) div Capacity,
            #token_mode{
                us_per_token = UsPerToken,
                max_time_us = MaxTimeUs
            };
        false ->
            #mini_token_mode{
                mini_tokens_per_ms = (1000 * Capacity) div IntervalMs,
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

%%--------------------------------------------------------------------
%% Atomic helpers
%%--------------------------------------------------------------------

atomic_add(#atomic_value{aref = ARef, index = Index}, Amount) ->
    atomics:add(ARef, Index, Amount).

atomic_sub(#atomic_value{aref = ARef, index = Index}, Amount) ->
    atomics:sub(ARef, Index, Amount).

atomic_get(#atomic_value{aref = ARef, index = Index}) ->
    atomics:get(ARef, Index).

atomic_sub_get(#atomic_value{aref = ARef, index = Index}, Amount) ->
    atomics:sub_get(ARef, Index, Amount).

atomic_put(#atomic_value{aref = ARef, index = Index}, Value) ->
    atomics:put(ARef, Index, Value).

atomic_compare_exchange(#atomic_value{aref = ARef, index = Index}, OldValue, NewValue) ->
    atomics:compare_exchange(ARef, Index, OldValue, NewValue).
