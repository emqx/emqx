%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_keepalive).

-include("types.hrl").

-export([
    init/1,
    init/2,
    init/3,
    info/1,
    info/2,
    check/1,
    check/2,
    update/3,
    hibernate/2,
    wakeup/2,
    check_deadline/1
]).

-elvis([{elvis_style, no_if_expression, disable}]).

-export_type([keepalive/0]).

-record(keepalive, {
    check_interval :: pos_integer(),
    %% the received packets since last keepalive check
    statval :: non_neg_integer(),
    %% stat reader func
    stat_reader :: mfargs() | undefined,
    %% Milliseconds without a received packet, as of the last check.
    idle_milliseconds = 0 :: non_neg_integer(),
    max_idle_millisecond :: pos_integer()
}).

-opaque keepalive() :: #keepalive{}.
-define(MAX_INTERVAL, 65535000).

%% @doc Init keepalive.
-spec init(Interval :: non_neg_integer()) -> keepalive().
init(Interval) -> init(default, 0, Interval).

init(Zone, Interval) ->
    RecvCnt = emqx_chan_stats:get(recv_pkt),
    init(Zone, RecvCnt, Interval).

%% from mqtt-v3.1.1 specific
%% A Keep Alive value of zero (0) has the effect of turning off the keep alive mechanism.
%% This means that, in this case, the Server is not required
%% to disconnect the Client on the grounds of inactivity.
%% Note that a Server is permitted to disconnect a Client that it determines
%% to be inactive or non-responsive at any time,
%% regardless of the Keep Alive value provided by that Client.
%%  Non normative comment
%%The actual value of the Keep Alive is application specific;
%% typically this is a few minutes.
%% The maximum value is (65535s) 18 hours 12 minutes and 15 seconds.
%% @doc Init keepalive.
-spec init(
    Zone :: atom(),
    StatVal :: non_neg_integer() | Reader :: mfargs(),
    Second :: non_neg_integer()
) -> keepalive() | undefined.
init(Zone, Stat, Second) when Second > 0 andalso Second =< ?MAX_INTERVAL ->
    #{check_interval := Interval, max_idle_millisecond := MaxIdleMs} =
        keepalive_params(Zone, Second),
    {StatVal, ReaderMFA} =
        case Stat of
            {M, F, A} = MFA ->
                {erlang:apply(M, F, A), MFA};
            Stat when is_integer(Stat) ->
                {Stat, undefined}
        end,
    #keepalive{
        check_interval = Interval,
        statval = StatVal,
        stat_reader = ReaderMFA,
        idle_milliseconds = 0,
        max_idle_millisecond = MaxIdleMs
    };
init(_Zone, _, 0) ->
    undefined;
init(Zone, StatVal, Interval) when Interval > ?MAX_INTERVAL -> init(Zone, StatVal, ?MAX_INTERVAL).

%% @doc Get Info of the keepalive.
-spec info(keepalive()) -> emqx_types:infos().
info(#keepalive{
    check_interval = Interval,
    statval = StatVal,
    idle_milliseconds = IdleIntervals,
    max_idle_millisecond = MaxMs
}) ->
    #{
        check_interval => Interval,
        statval => StatVal,
        idle_milliseconds => IdleIntervals,
        max_idle_millisecond => MaxMs
    }.

-spec info(check_interval | statval | idle_milliseconds, keepalive()) ->
    non_neg_integer() | undefined.
info(check_interval, #keepalive{check_interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(idle_milliseconds, #keepalive{idle_milliseconds = Val}) ->
    Val;
info(check_interval, undefined) ->
    0.

check(Keepalive = #keepalive{check_interval = Interval}) ->
    check(read_stat(Keepalive), Interval, Keepalive);
check(Keepalive) ->
    {ok, Keepalive}.

%% @doc Check keepalive.
-spec check(non_neg_integer(), keepalive()) ->
    {ok, keepalive()} | {error, timeout}.
check(NewVal, #keepalive{check_interval = Interval} = Keepalive) ->
    %% Called after the check_interval timer fires, so the elapsed time is Interval.
    check(NewVal, Interval, Keepalive).

check(NewVal, ElapsedMs, #keepalive{
    statval = NewVal, idle_milliseconds = IdleAcc, max_idle_millisecond = Max
}) when
    IdleAcc + ElapsedMs >= Max
->
    {error, timeout};
check(NewVal, ElapsedMs, Keepalive) ->
    {ok, advance(NewVal, ElapsedMs, Keepalive)}.

advance(NewVal, ElapsedMs, #keepalive{statval = NewVal, idle_milliseconds = IdleAcc} = Keepalive) ->
    Keepalive#keepalive{idle_milliseconds = IdleAcc + ElapsedMs};
advance(NewVal, _ElapsedMs, Keepalive) ->
    Keepalive#keepalive{statval = NewVal, idle_milliseconds = 0}.

%% idle_milliseconds does not change between hibernate/2 and wakeup/2, so this
%% is also the time the deadline timer was armed with.
till_deadline(#keepalive{idle_milliseconds = IdleAcc, max_idle_millisecond = Max}) ->
    max(Max - IdleAcc, 1).

read_stat(#keepalive{stat_reader = undefined}) ->
    emqx_chan_stats:get(recv_pkt);
read_stat(#keepalive{stat_reader = {M, F, A}}) ->
    erlang:apply(M, F, A).

-doc """
Switch from sampling to a single deadline before the process hibernates.

`SinceArmMs` is the time since the sampling timer was armed. Returns the
time to arm the timer with, so that it fires when the idle time reaches
`max_idle_millisecond`. The caller calls `wakeup/2` when the process wakes
up, also when the deadline timer has fired.
""".
-spec hibernate(non_neg_integer(), keepalive()) -> {pos_integer(), keepalive()}.
hibernate(SinceArmMs, Keepalive) ->
    NKeepalive = advance(read_stat(Keepalive), SinceArmMs, Keepalive),
    {till_deadline(NKeepalive), NKeepalive}.

-doc """
Credit the time the process was hibernated, after `hibernate/2`.

`RemainingMs` is the time that was left on the deadline timer, 0 when it
has fired.
""".
-spec wakeup(non_neg_integer(), keepalive()) -> keepalive().
wakeup(RemainingMs, Keepalive) ->
    SleptMs = max(till_deadline(Keepalive) - RemainingMs, 0),
    advance(read_stat(Keepalive), SleptMs, Keepalive).

-doc """
Check the keepalive when the deadline timer fires, after `wakeup/2` credited
the whole deadline.
""".
-spec check_deadline(keepalive()) -> {ok, keepalive()} | {error, timeout}.
check_deadline(Keepalive) ->
    check(read_stat(Keepalive), 0, Keepalive).

%% @doc Update keepalive while preserving stat_reader and idle time.
-spec update(atom(), non_neg_integer(), keepalive() | undefined) -> keepalive() | undefined.
update(Zone, Interval, undefined) ->
    init(Zone, 0, Interval);
update(Zone, Interval, Keepalive) when Interval > ?MAX_INTERVAL ->
    update(Zone, ?MAX_INTERVAL, Keepalive);
update(_Zone, 0, _Keepalive) ->
    undefined;
update(
    Zone,
    Interval,
    Keepalive
) ->
    #{check_interval := NewCheckInterval, max_idle_millisecond := MaxIdleMs} =
        keepalive_params(Zone, Interval),
    Keepalive#keepalive{
        check_interval = NewCheckInterval,
        max_idle_millisecond = MaxIdleMs
    }.

keepalive_params(Zone, Interval) ->
    #{keepalive_multiplier := Mul, keepalive_check_interval := CheckInterval} =
        emqx_config:get_zone_conf(Zone, [mqtt]),
    MilliSeconds = timer:seconds(Interval),
    #{
        check_interval => emqx_utils:clamp(CheckInterval, 1000, max(MilliSeconds div 2, 1000)),
        max_idle_millisecond => ceil(MilliSeconds * Mul)
    }.
