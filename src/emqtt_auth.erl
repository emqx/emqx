%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_auth).

-author('ery.lee@gmail.com').

-include("emqtt.hrl").

-include_lib("elog/include/elog.hrl").

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
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

check(Username, Password) ->
	gen_server2:call(?MODULE, {check, Username, Password}).

add(Username, Password) when is_binary(Username) ->
	gen_server2:call(?MODULE, {add, Username, Password}).

delete(Username) when is_binary(Username) ->
	gen_server2:cast(?MODULE, {delete, Username}).

init([]) ->
	{Name, Opts} = get_auth(),
	AuthMod = authmod(Name),
	ok = AuthMod:init(Opts),
	?INFO("authmod is ~p", [AuthMod]),
	?INFO("~p is started", [?MODULE]),
	{ok, #state{authmod=AuthMod, authopts=Opts}}.

get_auth() ->
	case application:get_env(auth) of
		{ok, Auth} -> Auth;
		undefined -> {anonymous, []}
	end.

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
