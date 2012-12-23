%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%
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
	KeepAlive#keep_alive{state=ative}.

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


