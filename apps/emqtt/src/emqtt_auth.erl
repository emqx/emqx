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

-module(emqtt_auth).

-author('ery.lee@gmail.com').

-include("emqtt.hrl").

-include("emqtt_log.hrl").


-export([start_link/0,
		add/2,
		check/2,
		delete/1]).

-behavior(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {authmod, authopts}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

check(Username, Password) ->
	gen_server:call(?MODULE, {check, Username, Password}).

add(Username, Password) when is_binary(Username) ->
	gen_server:call(?MODULE, {add, Username, Password}).

delete(Username) when is_binary(Username) ->
	gen_server:cast(?MODULE, {delete, Username}).

init([]) ->
	{ok, {Name, Opts}} = application:get_env(auth),
	AuthMod = authmod(Name),
	ok = AuthMod:init(Opts),
	?INFO("authmod is ~p", [AuthMod]),
	?INFO("~p is started", [?MODULE]),
	{ok, #state{authmod=AuthMod, authopts=Opts}}.

authmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqtt_auth_", Name])).

handle_call({check, Username, Password}, _From, #state{authmod=AuthMod} = State) ->
	{reply, AuthMod:check(Username, Password), State};

handle_call({add, Username, Password}, _From, #state{authmod=AuthMod} = State) ->
	{reply, AuthMod:add(Username, Password), State};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({delete, Username}, #state{authmod=AuthMod} = State) ->
	AuthMod:delete(Username),
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
