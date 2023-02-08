%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ init/1
        , info/1
        , info/2
        , check/2
        , set/3
        ]).

-export_type([keepalive/0]).
-elvis([{elvis_style, no_if_expression, disable}]).

-record(keepalive, {
          interval :: pos_integer(),
          statval  :: non_neg_integer(),
          repeat   :: non_neg_integer()
         }).

-opaque(keepalive() :: #keepalive{}).

%% @doc Init keepalive.
-spec(init(Interval :: non_neg_integer()) -> keepalive()).
init(Interval) when Interval > 0 ->
    #keepalive{interval = Interval,
               statval  = 0,
               repeat   = 0}.

%% @doc Get Info of the keepalive.
-spec(info(keepalive()) -> emqx_types:infos()).
info(#keepalive{interval = Interval,
                statval  = StatVal,
                repeat   = Repeat}) ->
    #{interval => Interval,
      statval  => StatVal,
      repeat   => Repeat
     }.

-spec(info(interval | statval | repeat, keepalive())
      -> non_neg_integer()).
info(interval, #keepalive{interval = Interval}) ->
    Interval;
info(statval, #keepalive{statval = StatVal}) ->
    StatVal;
info(repeat, #keepalive{repeat = Repeat}) ->
    Repeat.

%% @doc Check keepalive.
-spec(check(non_neg_integer(), keepalive())
      -> {ok, keepalive()} | {error, timeout}).
check(NewVal, KeepAlive = #keepalive{statval = OldVal,
                                     repeat  = Repeat}) ->
    if
        NewVal =/= OldVal ->
            {ok, KeepAlive#keepalive{statval = NewVal, repeat = 0}};
        Repeat < 1 ->
            {ok, KeepAlive#keepalive{repeat = Repeat + 1}};
        true -> {error, timeout}
    end.

-define(IS_KEEPALIVE(Interval), Interval >= 0 andalso Interval =< 65535000).
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

%% @doc Update keepalive interval
%% The keepalive() is undefined when connecting via keepalive=0.
-spec(set(interval, non_neg_integer(), keepalive() | undefined) -> keepalive()).
set(interval, Interval, undefined) when ?IS_KEEPALIVE(Interval) ->
    init(Interval);
set(interval, Interval, KeepAlive) when ?IS_KEEPALIVE(Interval) ->
    KeepAlive#keepalive{interval = Interval}.
