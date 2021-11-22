%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_stats).

-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

%% API
-export([ in/2, out/2, drop/2, expire/2
        , update/3, new/0, to_summary/1]).

-type stats() :: #{ held := integer()
                  , in := integer()
                  , out := integer()
                  , drop := integer()
                  , expired := integer()
                  , basis := basis_point()
                  , last_tick_time := pos_integer()
                  , index := undefined | index()
                  }.

-type summary() :: #{ held := integer()
                    , out := integer()
                    , drop := integer()
                    , expired := integer()
                    }.

-define(NOW, erlang:system_time(millisecond)).

-export_type([stats/0, summary/0]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec new() -> stats().
new() ->
    #{ held => 0
     , in => 0
     , out => 0
     , drop => 0
     , expired => 0
     , basis => 0
     , last_tick_time => ?NOW
     , index => undefined}.

-spec in(pos_integer(), stats()) -> stats().
in(Inc, #{in := In} = S) ->
    S#{in := In + Inc}.

-spec out(pos_integer(), stats()) -> stats().
out(Inc, #{out := Out} = S) ->
    S#{out := Out + Inc}.

-spec drop(pos_integer(), stats()) -> stats().
drop(Inc, #{drop := Drop} = S) ->
    S#{drop := Drop + Inc}.

-spec expire(pos_integer(), stats()) -> stats().
expire(Inc, #{expire := Expire} = S) ->
    S#{expire := Expire + Inc}.

-spec update(pos_integer(), stats(), emqx_channel:channel()) -> stats().
update(Interval,
       #{held := Held, in := In, out := Out, expired := Expired,
         drop := Drop, last_tick_time := LastTickTime} = S,
       Channel) ->
    io:format(">>>>> UPDATE:~p~n", [S]),
    Now = ?NOW,
    Period = (Now - LastTickTime) / Interval,
    Held2 = Held + In,
    AllOut = Out + Drop, %% does need add expired ???
    NewBasis = calc_basis(Held2, AllOut, Period),
    S2 = call_hook(NewBasis, S, Channel),
    io:format(">>>> held2:~p, Out:~p, Drop:~p, AllOut:~p Expired:~p~n",
              [Held2 - AllOut - Expired, Out, Drop, AllOut, Expired]             ),
    S2#{ held := Held2 - AllOut - Expired
       , in := 0
       , out := 0
       , drop := 0
       , expired := 0
       , basis := NewBasis
       , last_tick_time := Now
       }.

-spec to_summary(stats()) -> summary().
to_summary(Stats) ->
    maps:without([last_tick_time, index], Stats).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec call_hook(basis_point(), stats(), slow_subs_stats_args()) -> stats().
%% when degree of congestion is zero, and sub not in topk, no need call hook
call_hook(0, #{index := undefined} = Stats, _Args) ->
    Stats;

call_hook(_, Stats, Args) ->
    Index = emqx:run_fold_hook('message.slow_subs_stats', [Stats, Args], undefined),
    Stats#{index := Index}.

calc_basis(Held, Out, Period) ->
    Raw = calc_raw_basis(Held, Held - Out),
    ?TO_BASIS(Raw * Period).

calc_raw_basis(_, 0) ->
    0;
calc_raw_basis(Held, UnOut) ->
    Held / UnOut.
