%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        ]).

-export_type([keepalive/0]).

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

-spec(info(interval|statval|repeat, keepalive())
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

