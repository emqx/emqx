%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("src/stomp/include/emqx_stomp.hrl").

-export([
    init/1,
    check/3,
    reset/3,
    info/1,
    interval/2
]).

-record(heartbeater, {interval, statval, repeat}).

-type name() :: incoming | outgoing.

-type heartbeat() :: #{
    incoming => #heartbeater{},
    outgoing => #heartbeater{}
}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec init({non_neg_integer(), non_neg_integer()}) -> heartbeat().
init({0, 0}) ->
    #{};
init({Cx, Cy}) ->
    maps:filter(
        fun(_, V) -> V /= undefined end,
        #{
            incoming => heartbeater(Cx),
            outgoing => heartbeater(Cy)
        }
    ).

heartbeater(0) ->
    undefined;
heartbeater(I) ->
    #heartbeater{
        interval = I,
        statval = 0,
        repeat = 0
    }.

-spec check(name(), pos_integer(), heartbeat()) ->
    {ok, heartbeat()}
    | {error, timeout}.
check(Name, NewVal, HrtBt) ->
    HrtBter = maps:get(Name, HrtBt),
    case check(NewVal, HrtBter) of
        {error, _} = R -> R;
        {ok, NHrtBter} -> {ok, HrtBt#{Name => NHrtBter}}
    end.

check(
    NewVal,
    HrtBter = #heartbeater{
        statval = OldVal,
        repeat = Repeat
    }
) ->
    if
        NewVal =/= OldVal ->
            {ok, HrtBter#heartbeater{statval = NewVal, repeat = 0}};
        Repeat < 1 ->
            {ok, HrtBter#heartbeater{repeat = Repeat + 1}};
        true ->
            {error, timeout}
    end.

-spec reset(name(), pos_integer(), heartbeat()) ->
    heartbeat().
reset(Name, NewVal, HrtBt) ->
    HrtBter = maps:get(Name, HrtBt),
    HrtBt#{Name => reset(NewVal, HrtBter)}.

reset(NewVal, HrtBter) ->
    HrtBter#heartbeater{statval = NewVal, repeat = 1}.

-spec info(heartbeat()) -> map().
info(HrtBt) ->
    maps:map(
        fun(
            _,
            #heartbeater{
                interval = Intv,
                statval = Val,
                repeat = Repeat
            }
        ) ->
            #{interval => Intv, statval => Val, repeat => Repeat}
        end,
        HrtBt
    ).

interval(Type, HrtBt) ->
    case maps:get(Type, HrtBt, undefined) of
        undefined -> undefined;
        #heartbeater{interval = Intv} -> Intv
    end.
