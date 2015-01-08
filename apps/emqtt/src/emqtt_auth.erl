%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@emqtt.io>
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

-module(emqtt_auth).

-author('feng@emqtt.io').

-include("emqtt.hrl").

-export([start_link/0,
		add/2,
		check/1, check/2,
		delete/1]).

-behavior(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-define(TAB, ?MODULE).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check({Usename :: binary(), Password :: binary()}) -> true | false.
check({Username, Password}) ->
	execute(check, [Username, Password]).

-spec check(Usename :: binary(), Password :: binary()) -> true | false.
check(Username, Password) ->
	execute(check, [Username, Password]).

-spec add(Usename :: binary(), Password :: binary()) -> ok.
add(Username, Password) ->
	execute(add, [Username, Password]).

-spec delete(Username :: binary()) -> ok.
delete(Username) ->
	execute(delete, [Username]).

execute(F, Args) ->
	[{_, M}] = ets:lookup(?TAB, mod), 
	apply(M, F, Args).

init([]) ->
	{ok, {Name, Opts}} = application:get_env(auth),
	AuthMod = authmod(Name),
	ok = AuthMod:init(Opts),
	ets:new(?TAB, [named_table, protected]),
	ets:insert(?TAB, {mod, AuthMod}),
	{ok, undefined}.

authmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqtt_auth_", Name])).

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

