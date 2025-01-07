%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    update/3
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
    max_idle_millisecond :: pos_integer()
}).

-opaque keepalive() :: #keepalive{}.
-define(MAX_INTERVAL, 65535000).

%% @doc Init keepalive.
-spec init(Interval :: non_neg_integer()) -> keepalive().
init(Interval) -> init(default, 0, Interval).

init(Zone, Interval) ->
    RecvCnt = emqx_pd:get_counter(recv_pkt),
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
    StatVal :: non_neg_integer() | Reader :: mfa(),
    Second :: non_neg_integer()
) -> keepalive() | undefined.
init(Zone, Stat, Second) when Second > 0 andalso Second =< ?MAX_INTERVAL ->
    #{keepalive_multiplier := Mul, keepalive_check_interval := CheckInterval} =
        emqx_config:get_zone_conf(Zone, [mqtt]),
    MilliSeconds = timer:seconds(Second),
    Interval = emqx_utils:clamp(CheckInterval, 1000, max(MilliSeconds div 2, 1000)),
    MaxIdleMs = ceil(MilliSeconds * Mul),
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
    non_neg_integer().
info(check_interval, #keepalive{check_interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(idle_milliseconds, #keepalive{idle_milliseconds = Val}) ->
    Val;
info(check_interval, undefined) ->
    0.

check(Keepalive = #keepalive{stat_reader = undefined}) ->
    RecvCnt = emqx_pd:get_counter(recv_pkt),
    check(RecvCnt, Keepalive);
check(Keepalive = #keepalive{stat_reader = {M, F, A}}) ->
    RecvCnt = erlang:apply(M, F, A),
    check(RecvCnt, Keepalive);
check(Keepalive) ->
    {ok, Keepalive}.

%% @doc Check keepalive.
-spec check(non_neg_integer(), keepalive()) ->
    {ok, keepalive()} | {error, timeout}.

check(
    NewVal,
    #keepalive{
        statval = NewVal,
        idle_milliseconds = IdleAcc,
        check_interval = Interval,
        max_idle_millisecond = Max
    }
) when IdleAcc + Interval >= Max ->
    {error, timeout};
check(
    NewVal,
    #keepalive{
        statval = NewVal,
        idle_milliseconds = IdleAcc,
        check_interval = Interval
    } = KeepAlive
) ->
    {ok, KeepAlive#keepalive{statval = NewVal, idle_milliseconds = IdleAcc + Interval}};
check(NewVal, #keepalive{} = KeepAlive) ->
    {ok, KeepAlive#keepalive{statval = NewVal, idle_milliseconds = 0}}.

%% @doc Update keepalive.
%% The statval of the previous keepalive will be used,
%% and normal checks will begin from the next cycle.
-spec update(atom(), non_neg_integer(), keepalive() | undefined) -> keepalive() | undefined.
update(Zone, Interval, undefined) -> init(Zone, 0, Interval);
update(Zone, Interval, #keepalive{statval = StatVal}) -> init(Zone, StatVal, Interval).
