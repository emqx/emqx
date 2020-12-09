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

%% @doc Stomp heartbeat.
-module(emqx_stomp_heartbeat).

-include("emqx_stomp.hrl").

-export([ start_link/2
        , stop/1
        ]).

%% callback
-export([ init/1
        , loop/3
        ]).

-define(MAX_REPEATS, 1).

-record(heartbeater, {name, cycle, tref, val, statfun, action, repeat = 0}).

start_link({0, _, _}, {0, _, _}) ->
    {ok, none};

start_link(Incoming, Outgoing) ->
    Params = [self(), Incoming, Outgoing],
    {ok, spawn_link(?MODULE, init, [Params])}.

stop(Pid) ->
    Pid ! stop.

init([Parent, Incoming, Outgoing]) ->
    loop(Parent, heartbeater(incomming, Incoming), heartbeater(outgoing,  Outgoing)).

heartbeater(_, {0, _, _}) ->
    undefined;

heartbeater(InOut, {Cycle, StatFun, ActionFun}) ->
    {ok, Val} = StatFun(),
    #heartbeater{name = InOut, cycle = Cycle,
                 tref = timer(InOut, Cycle),
                 val = Val, statfun = StatFun,
                 action = ActionFun}.

loop(Parent, Incomming, Outgoing) ->
    receive
        {heartbeat, incomming} ->
            #heartbeater{val = LastVal, statfun = StatFun,
                         action = Action, repeat = Repeat} = Incomming,
            case StatFun() of
                {ok, Val} ->
                    if Val =/= LastVal ->
                           hibernate([Parent, resume(Incomming, Val), Outgoing]);
                       Repeat < ?MAX_REPEATS ->
                           hibernate([Parent, resume(Incomming, Val, Repeat+1), Outgoing]);
                       true ->
                           Action()
                    end;
                {error, Error} -> %% einval
                    exit({shutdown, Error})
            end;
        {heartbeat, outgoing}  ->
            #heartbeater{val = LastVal, statfun = StatFun, action = Action} = Outgoing,
            case StatFun() of
                {ok, Val} ->
                    if Val =:= LastVal ->
                           Action(), {ok, NewVal} = StatFun(),
                           hibernate([Parent, Incomming, resume(Outgoing, NewVal)]);
                       true ->
                           hibernate([Parent, Incomming, resume(Outgoing, Val)])
                    end;
                {error, Error} -> %% einval
                    exit({shutdown, Error})
            end;
        stop ->
            ok;
        _Other ->
            loop(Parent, Incomming, Outgoing)
    end.

resume(Hb, NewVal) ->
    resume(Hb, NewVal, 0).
resume(Hb = #heartbeater{name = InOut, cycle = Cycle}, NewVal, Repeat) ->
    Hb#heartbeater{tref = timer(InOut, Cycle), val = NewVal, repeat = Repeat}.

timer(_InOut, 0) ->
    undefined;
timer(InOut, Cycle) ->
    erlang:send_after(Cycle, self(), {heartbeat, InOut}).

hibernate(Args) ->
    erlang:hibernate(?MODULE, loop, Args).

