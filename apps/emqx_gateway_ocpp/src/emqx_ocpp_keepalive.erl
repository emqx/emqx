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

%% copied from emqx_keepalive module, but made some broken changes
-module(emqx_ocpp_keepalive).

-export([
    init/1,
    init/2,
    info/1,
    info/2,
    check/2,
    set/3
]).

-export_type([keepalive/0]).
-elvis([{elvis_style, no_if_expression, disable}]).

-record(keepalive, {
    interval :: pos_integer(),
    statval :: non_neg_integer(),
    repeat :: non_neg_integer(),
    max_repeat :: non_neg_integer()
}).

-opaque keepalive() :: #keepalive{}.

%% @doc Init keepalive.
-spec init(Interval :: non_neg_integer()) -> keepalive().
init(Interval) when Interval > 0 ->
    init(Interval, 1).

-spec init(Interval :: non_neg_integer(), MaxRepeat :: non_neg_integer()) -> keepalive().
init(Interval, MaxRepeat) when
    Interval > 0, MaxRepeat >= 0
->
    #keepalive{
        interval = Interval,
        statval = 0,
        repeat = 0,
        max_repeat = MaxRepeat
    }.

%% @doc Get Info of the keepalive.
-spec info(keepalive()) -> emqx_types:infos().
info(#keepalive{
    interval = Interval,
    statval = StatVal,
    repeat = Repeat,
    max_repeat = MaxRepeat
}) ->
    #{
        interval => Interval,
        statval => StatVal,
        repeat => Repeat,
        max_repeat => MaxRepeat
    }.

-spec info(interval | statval | repeat, keepalive()) ->
    non_neg_integer().
info(interval, #keepalive{interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(repeat, #keepalive{repeat = Repeat}) ->
    Repeat;
info(max_repeat, #keepalive{max_repeat = MaxRepeat}) ->
    MaxRepeat.

%% @doc Check keepalive.
-spec check(non_neg_integer(), keepalive()) ->
    {ok, keepalive()} | {error, timeout}.
check(
    NewVal,
    KeepAlive = #keepalive{
        statval = OldVal,
        repeat = Repeat,
        max_repeat = MaxRepeat
    }
) ->
    if
        NewVal =/= OldVal ->
            {ok, KeepAlive#keepalive{statval = NewVal, repeat = 0}};
        Repeat < MaxRepeat ->
            {ok, KeepAlive#keepalive{repeat = Repeat + 1}};
        true ->
            {error, timeout}
    end.

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

%% @doc Update keepalive's interval
-spec set(interval, non_neg_integer(), keepalive()) -> keepalive().
set(interval, Interval, KeepAlive) when Interval >= 0 andalso Interval =< 65535000 ->
    KeepAlive#keepalive{interval = Interval}.
