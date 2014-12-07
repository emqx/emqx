%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng.lee@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
-module(emqtt_keep_alive).

-export([new/2,
		state/1,
		activate/1,
		reset/1,
		cancel/1]).

-record(keep_alive, {state, period, timer, msg}).

new(undefined, _) -> 
	undefined;
new(0, _) -> 
	undefined;
new(Period, TimeoutMsg) when is_integer(Period) ->
	Ref = erlang:send_after(Period, self(), TimeoutMsg),
	#keep_alive{state=idle, period=Period, timer=Ref, msg=TimeoutMsg}.

state(undefined) -> 
	undefined;
state(#keep_alive{state=State}) ->
	State.

activate(undefined) -> 
	undefined; 
activate(KeepAlive) when is_record(KeepAlive, keep_alive) -> 
	KeepAlive#keep_alive{state=active}.

reset(undefined) ->
	undefined;
reset(KeepAlive=#keep_alive{period=Period, timer=Timer, msg=Msg}) ->
	catch erlang:cancel_timer(Timer),
	Ref = erlang:send_after(Period, self(), Msg),
	KeepAlive#keep_alive{state=idle, timer = Ref}.

cancel(undefined) -> 
	undefined;
cancel(KeepAlive=#keep_alive{timer=Timer}) -> 
	catch erlang:cancel_timer(Timer),
	KeepAlive#keep_alive{timer=undefined}.


