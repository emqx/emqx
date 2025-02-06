%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    init/1,
    check/3,
    reset/3,
    info/1,
    interval/2
]).

-export_type([heartbeat/0]).

-record(heartbeater, {interval, statval, repeat}).

-type name() :: incoming | outgoing.

-type heartbeat() :: #{
    incoming => #heartbeater{},
    outgoing => #heartbeater{}
}.

-elvis([{elvis_style, no_if_expression, disable}]).

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
            %% Allow the first check to pass
            %% This is to compensate network latency etc for the heartbeat.
            %% On average (normal distribution of the timing of when check
            %% happen during the heartbeat interval), it allows the connection
            %% to idle for 1.5x heartbeat interval.
            {ok, HrtBter#heartbeater{repeat = Repeat + 1}};
        true ->
            {error, timeout}
    end.

%% @doc Call this when heartbeat is receved or sent.
-spec reset(name(), pos_integer(), heartbeat()) ->
    heartbeat().
reset(Name, NewVal, HrtBt) ->
    HrtBter = maps:get(Name, HrtBt),
    HrtBt#{Name => reset(NewVal, HrtBter)}.

%% Set counter back to zero (the initial state)
reset(NewVal, HrtBter) ->
    HrtBter#heartbeater{statval = NewVal, repeat = 0}.

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
