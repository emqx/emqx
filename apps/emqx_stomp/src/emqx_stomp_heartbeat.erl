%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Stomp heartbeat.
-module(emqx_stomp_heartbeat).

-include("emqx_stomp.hrl").

-export([ init/1
        , check/3
        , info/1
        , interval/2
        , reset/3
        ]).

-record(heartbeater, {interval, statval, repeat, repeat_max}).

-type name() :: incoming | outgoing.

-type heartbeat() :: #{incoming => #heartbeater{},
                       outgoing => #heartbeater{}
                      }.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec init({non_neg_integer(), non_neg_integer()}) -> heartbeat().
init({0, 0}) ->
    #{};
init({Cx, Cy}) ->
    maps:filter(fun(_, V) -> V /= undefined end,
      #{incoming => heartbeater(incoming, Cx),
        outgoing => heartbeater(outgoing, Cy)
       }).

heartbeater(_, 0) ->
    undefined;
heartbeater(N, I) ->
    #heartbeater{
       interval = I,
       statval = 0,
       repeat = 0,
       repeat_max = repeat_max(N)
      }.

repeat_max(incoming) -> 1;
repeat_max(outgoing) -> 0.

-spec check(name(), pos_integer(), heartbeat())
    -> {ok, heartbeat()}
     | {error, timeout}.
check(Name, NewVal, HrtBt) ->
    HrtBter = maps:get(Name, HrtBt),
    case check(NewVal, HrtBter) of
        {error, _} = R -> R;
        {ok, NHrtBter} ->
            {ok, HrtBt#{Name => NHrtBter}}
    end.

check(NewVal, HrtBter = #heartbeater{statval = OldVal,
                                     repeat = Repeat,
                                     repeat_max = Max}) ->
    if
        NewVal =/= OldVal ->
            {ok, HrtBter#heartbeater{statval = NewVal, repeat = 0}};
        Repeat < Max ->
            {ok, HrtBter#heartbeater{repeat = Repeat + 1}};
        true -> {error, timeout}
    end.

-spec info(heartbeat()) -> map().
info(HrtBt) ->
    maps:map(fun(_, #heartbeater{interval = Intv,
                                 statval = Val,
                                 repeat = Repeat}) ->
            #{interval => Intv, statval => Val, repeat => Repeat}
             end, HrtBt).

interval(Type, HrtBt) ->
    case maps:get(Type, HrtBt, undefined) of
        undefined -> undefined;
        #heartbeater{interval = Intv} -> Intv
    end.

reset(Type, StatVal, HrtBt) ->
    case maps:get(Type, HrtBt, undefined) of
        undefined -> HrtBt;
        HrtBter ->
            HrtBt#{Type => HrtBter#heartbeater{statval = StatVal, repeat = 0}}
    end.
