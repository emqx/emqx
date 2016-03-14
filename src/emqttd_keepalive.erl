%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc Client Keepalive
-module(emqttd_keepalive).

-export([start/3, check/1, cancel/1]).

-record(keepalive, {statfun, statval,
                    tsec, tmsg, tref,
                    repeat = 0}).

-type keepalive() :: #keepalive{}.

%% @doc Start a keepalive
-spec(start(fun(), integer(), any()) -> undefined | keepalive()).
start(_, 0, _) ->
    undefined;
start(StatFun, TimeoutSec, TimeoutMsg) ->
    {ok, StatVal} = StatFun(),
    #keepalive{statfun = StatFun, statval = StatVal,
               tsec = TimeoutSec, tmsg = TimeoutMsg,
               tref = timer(TimeoutSec, TimeoutMsg)}.

%% @doc Check keepalive, called when timeout.
-spec(check(keepalive()) -> {ok, keepalive()} | {error, any()}).
check(KeepAlive = #keepalive{statfun = StatFun, statval = LastVal, repeat = Repeat}) ->
    case StatFun() of
        {ok, NewVal} ->
            if NewVal =/= LastVal ->
                    {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = 0})};
                Repeat < 1 ->
                    {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = Repeat + 1})};
                true ->
                    {error, timeout}
            end;
        {error, Error} ->
            {error, Error}
    end.

resume(KeepAlive = #keepalive{tsec = TimeoutSec, tmsg = TimeoutMsg}) ->
    KeepAlive#keepalive{tref = timer(TimeoutSec, TimeoutMsg)}.

%% @doc Cancel Keepalive
-spec(cancel(keepalive()) -> ok).
cancel(#keepalive{tref = TRef}) ->
    cancel(TRef);
cancel(undefined) -> 
    ok;
cancel(TRef) ->
    catch erlang:cancel_timer(TRef).

timer(Sec, Msg) ->
    erlang:send_after(timer:seconds(Sec), self(), Msg).

