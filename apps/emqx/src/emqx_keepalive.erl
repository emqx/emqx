%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    init/1,
    init/2,
    info/1,
    info/2,
    check/2,
    update/2
]).

-elvis([{elvis_style, no_if_expression, disable}]).

-export_type([keepalive/0]).

-record(keepalive, {
    interval :: pos_integer(),
    statval :: non_neg_integer()
}).

-opaque keepalive() :: #keepalive{}.
-define(MAX_INTERVAL, 65535000).

%% @doc Init keepalive.
-spec init(Interval :: non_neg_integer()) -> keepalive().
init(Interval) -> init(0, Interval).

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
-spec init(StatVal :: non_neg_integer(), Interval :: non_neg_integer()) -> keepalive() | undefined.
init(StatVal, Interval) when Interval > 0 andalso Interval =< ?MAX_INTERVAL ->
    #keepalive{interval = Interval, statval = StatVal};
init(_, 0) ->
    undefined;
init(StatVal, Interval) when Interval > ?MAX_INTERVAL -> init(StatVal, ?MAX_INTERVAL).

%% @doc Get Info of the keepalive.
-spec info(keepalive()) -> emqx_types:infos().
info(#keepalive{
    interval = Interval,
    statval = StatVal
}) ->
    #{
        interval => Interval,
        statval => StatVal
    }.

-spec info(interval | statval, keepalive()) ->
    non_neg_integer().
info(interval, #keepalive{interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(interval, undefined) ->
    0.

%% @doc Check keepalive.
-spec check(non_neg_integer(), keepalive()) ->
    {ok, keepalive()} | {error, timeout}.
check(Val, #keepalive{statval = Val}) -> {error, timeout};
check(Val, KeepAlive) -> {ok, KeepAlive#keepalive{statval = Val}}.

%% @doc Update keepalive.
%% The statval of the previous keepalive will be used,
%% and normal checks will begin from the next cycle.
-spec update(non_neg_integer(), keepalive() | undefined) -> keepalive() | undefined.
update(Interval, undefined) -> init(0, Interval);
update(Interval, #keepalive{statval = StatVal}) -> init(StatVal, Interval).
