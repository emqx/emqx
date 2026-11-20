%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Delay for the Dashboard password check.
%%
%% This module keeps a per-node sample of the time that one password
%% hash check takes at the default cost.  The sample is kept in the
%% application environment together with the time it was taken.
%%
%% - `measure/1' runs a password hash check at the default cost and
%%   records its time as the new sample when the stored sample is
%%   older than `?SAMPLE_MAX_AGE_MS'.  It measures the check only,
%%   never the delay that comes after it.
%% - `after_check/0' sleeps for a random time from 0 to
%%   `?EXTRA_PERCENT' percent of the sample.
%% - `without_check/0' sleeps for the sample plus the same random
%%   extra time.  The caller uses it when it does not run a hash
%%   check at the default cost.
%%
%% The sample is clamped to `?SAMPLE_MAX_US'.
-module(emqx_dashboard_check_delay).

-export([
    measure/1,
    after_check/0,
    without_check/0
]).

%% Exported so that tests can observe and replace it.
-export([sleep_us/1]).

%% For tests.
-export([
    sample/0,
    record_sample/1,
    set_sample/2,
    clear_sample/0,
    max_sample_us/0,
    max_age_ms/0,
    extra_percent/0
]).

-define(APP, emqx_dashboard).
-define(SAMPLE_KEY, password_check_sample).
-define(SAMPLE_MAX_AGE_MS, 60_000).
-define(SAMPLE_MAX_US, 500_000).
-define(EXTRA_PERCENT, 5).

-type sample() :: {SampleUs :: non_neg_integer(), TakenAtMs :: integer()}.

%% @doc Run `CheckFun', a password hash check at the default cost.
%% Record its run time as the sample when the stored sample is missing
%% or older than `?SAMPLE_MAX_AGE_MS'.  Return the result of `CheckFun'.
-spec measure(fun(() -> Result)) -> Result.
measure(CheckFun) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = CheckFun(),
    ElapsedUs = erlang:monotonic_time(microsecond) - T0,
    case is_sample_fresh(sample()) of
        true -> ok;
        false -> record_sample(ElapsedUs)
    end,
    Result.

%% @doc Sleep for a random time from 0 to `?EXTRA_PERCENT' percent of
%% the sample.  Call this after a password hash check at the default cost.
-spec after_check() -> ok.
after_check() ->
    SampleUs = ensure_sample(),
    ?MODULE:sleep_us(extra_us(SampleUs)).

%% @doc Sleep for the sample plus a random time from 0 to
%% `?EXTRA_PERCENT' percent of the sample.  Call this when no password
%% hash check at the default cost ran.  When there is no sample yet,
%% run one hash at the default cost to take it, and add only the
%% random extra time.
-spec without_check() -> ok.
without_check() ->
    case sample() of
        {SampleUs, _TakenAt} ->
            ?MODULE:sleep_us(SampleUs + extra_us(SampleUs));
        undefined ->
            SampleUs = seed_sample(),
            ?MODULE:sleep_us(extra_us(SampleUs))
    end.

-spec sleep_us(non_neg_integer()) -> ok.
sleep_us(DelayUs) ->
    %% `timer:sleep/1' takes whole milliseconds.
    timer:sleep((DelayUs + 500) div 1000).

-spec sample() -> sample() | undefined.
sample() ->
    application:get_env(?APP, ?SAMPLE_KEY, undefined).

-spec record_sample(non_neg_integer()) -> ok.
record_sample(SampleUs) ->
    set_sample(SampleUs, erlang:monotonic_time(millisecond)).

-spec set_sample(non_neg_integer(), integer()) -> ok.
set_sample(SampleUs, TakenAtMs) ->
    application:set_env(?APP, ?SAMPLE_KEY, {min(SampleUs, ?SAMPLE_MAX_US), TakenAtMs}).

-spec clear_sample() -> ok.
clear_sample() ->
    application:unset_env(?APP, ?SAMPLE_KEY).

max_sample_us() -> ?SAMPLE_MAX_US.

max_age_ms() -> ?SAMPLE_MAX_AGE_MS.

extra_percent() -> ?EXTRA_PERCENT.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_sample_fresh({_SampleUs, TakenAtMs}) ->
    erlang:monotonic_time(millisecond) - TakenAtMs < ?SAMPLE_MAX_AGE_MS;
is_sample_fresh(undefined) ->
    false.

ensure_sample() ->
    case sample() of
        {SampleUs, _TakenAt} -> SampleUs;
        undefined -> seed_sample()
    end.

%% Run one password hash at the default cost and record its time.
seed_sample() ->
    Input = crypto:strong_rand_bytes(16),
    measure(fun() -> emqx_dashboard_admin:hash(Input) end),
    {SampleUs, _TakenAt} = sample(),
    SampleUs.

extra_us(SampleUs) ->
    MaxExtraUs = SampleUs * ?EXTRA_PERCENT div 100,
    rand:uniform(MaxExtraUs + 1) - 1.
