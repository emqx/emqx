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
    wakeup/2
]).

-elvis([{elvis_style, no_if_expression, disable}]).

-export_type([keepalive/0]).

-record(keepalive, {
    check_interval :: pos_integer(),
    %% the received packets since last keepalive check
    statval :: non_neg_integer(),
    %% stat reader func
    stat_reader :: mfargs() | undefined,
    %% The number of idle intervals allowed before disconnecting the client.
    idle_milliseconds = 0 :: non_neg_integer(),
    max_idle_millisecond :: pos_integer(),
    %% The length of the armed timer while it is a deadline, see hibernate/2.
    deadline :: pos_integer() | undefined
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

-spec info(check_interval | statval | idle_milliseconds | deadline, keepalive()) ->
    non_neg_integer() | undefined.
info(check_interval, #keepalive{check_interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(idle_milliseconds, #keepalive{idle_milliseconds = Val}) ->
    Val;
info(deadline, #keepalive{deadline = Deadline}) ->
    Deadline;
info(check_interval, undefined) ->
    0.

check(Keepalive = #keepalive{deadline = undefined, check_interval = Interval}) ->
    check(read_stat(Keepalive), Interval, Keepalive);
check(Keepalive = #keepalive{deadline = Deadline}) ->
    check(read_stat(Keepalive), Deadline, Keepalive#keepalive{deadline = undefined});
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

read_stat(#keepalive{stat_reader = undefined}) ->
    emqx_chan_stats:get(recv_pkt);
read_stat(#keepalive{stat_reader = {M, F, A}}) ->
    erlang:apply(M, F, A).

-doc """
Switch from sampling to a single deadline before the process hibernates.

`SinceArmMs` is the time since the sampling timer was armed. Returns the
time to arm the timer with, so that it fires when the idle time reaches
`max_idle_millisecond`. The next `check/1` credits this time instead of
`check_interval`.
""".
-spec hibernate(non_neg_integer(), keepalive()) -> {pos_integer(), keepalive()}.
hibernate(SinceArmMs, Keepalive = #keepalive{deadline = undefined}) ->
    NKeepalive =
        #keepalive{idle_milliseconds = IdleAcc, max_idle_millisecond = Max} =
        advance(read_stat(Keepalive), SinceArmMs, Keepalive),
    Deadline = max(Max - IdleAcc, 1),
    {Deadline, NKeepalive#keepalive{deadline = Deadline}}.

-doc """
Switch back to sampling when the process wakes up before the deadline.

`RemainingMs` is the time that was left on the deadline timer.
""".
-spec wakeup(non_neg_integer(), keepalive()) -> keepalive().
wakeup(RemainingMs, Keepalive = #keepalive{deadline = Deadline}) when is_integer(Deadline) ->
    advance(read_stat(Keepalive), max(Deadline - RemainingMs, 0), Keepalive#keepalive{
        deadline = undefined
    }).

%% @doc Update keepalive while preserving stat_reader and idle time.
-spec update(atom(), non_neg_integer(), keepalive() | undefined) -> keepalive() | undefined.
update(Zone, Interval, undefined) ->
    init(Zone, 0, Interval);
update(Zone, Interval, Keepalive) when Interval > ?MAX_INTERVAL ->
    update(Zone, ?MAX_INTERVAL, Keepalive);
update(_Zone, 0, _Keepalive) ->
    undefined;
update(Zone, Interval, Keepalive = #keepalive{idle_milliseconds = IdleAcc, deadline = Deadline}) ->
    #{check_interval := NewCheckInterval, max_idle_millisecond := MaxIdleMs} =
        keepalive_params(Zone, Interval),
    %% A deadline is still set only when it has fired, and the caller re-arms
    %% the timer for sampling.
    Keepalive#keepalive{
        check_interval = NewCheckInterval,
        max_idle_millisecond = MaxIdleMs,
        idle_milliseconds = IdleAcc + fired_deadline(Deadline),
        deadline = undefined
    }.

fired_deadline(undefined) -> 0;
fired_deadline(Deadline) -> Deadline.

keepalive_params(Zone, Interval) ->
    #{keepalive_multiplier := Mul, keepalive_check_interval := CheckInterval} =
        emqx_config:get_zone_conf(Zone, [mqtt]),
    MilliSeconds = timer:seconds(Interval),
    #{
        check_interval => emqx_utils:clamp(CheckInterval, 1000, max(MilliSeconds div 2, 1000)),
        max_idle_millisecond => ceil(MilliSeconds * Mul)
    }.
