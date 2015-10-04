%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc client keepalive
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_keepalive).

-author("Feng Lee <feng@emqtt.io>").

-export([start/3, check/1, cancel/1]).

-record(keepalive, {statfun, statval,
                    tsec, tmsg, tref,
                    repeat = 0}).

%%------------------------------------------------------------------------------
%% @doc Start a keepalive
%% @end
%%------------------------------------------------------------------------------
start(_, 0, _) ->
    undefined;
start(StatFun, TimeoutSec, TimeoutMsg) ->
    {ok, StatVal} = StatFun(),
	#keepalive{statfun = StatFun, statval = StatVal,
               tsec = TimeoutSec, tmsg = TimeoutMsg,
               tref = timer(TimeoutSec, TimeoutMsg)}.

%%------------------------------------------------------------------------------
%% @doc Check keepalive, called when timeout.
%% @end
%%------------------------------------------------------------------------------
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

%%------------------------------------------------------------------------------
%% @doc Cancel Keepalive
%% @end
%%------------------------------------------------------------------------------
cancel(#keepalive{tref = TRef}) ->
    cancel(TRef);
cancel(undefined) -> 
	ok;
cancel(TRef) ->
	catch erlang:cancel_timer(TRef).

timer(Sec, Msg) ->
    erlang:send_after(timer:seconds(Sec), self(), Msg).

