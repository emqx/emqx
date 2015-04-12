%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% @doc
%%% emqttd client manager.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_cm).

-author('feng@emqtt.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(CLIENT_TABLE, mqtt_client).

%% API Exports 
-export([start_link/0]).

-export([lookup/1, register/1, unregister/1]).

%% Stats 
-export([getstats/0]).

%% gen_server Function Exports
-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {tab}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start client manager.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% Lookup client pid with clientId.
%%
%% @end
%%------------------------------------------------------------------------------
-spec lookup(ClientId :: binary()) -> pid() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
	case ets:lookup(?CLIENT_TABLE, ClientId) of
	[{_, Pid, _}] -> Pid;
	[] -> undefined
	end.

%%------------------------------------------------------------------------------
%% @doc Register clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec register(ClientId :: binary()) -> ok.
register(ClientId) when is_binary(ClientId) ->
    Pid = self(),
    %% this is atomic
    case ets:insert_new(?CLIENT_TABLE, {ClientId, Pid, undefined}) of
        true -> gen_server:cast(?SERVER, {monitor, ClientId, Pid});
        false -> gen_server:cast(?SERVER, {register, ClientId, Pid})
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Unregister clientId with pid.
%%
%% @end
%%------------------------------------------------------------------------------
-spec unregister(ClientId :: binary()) -> ok.
unregister(ClientId) when is_binary(ClientId) ->
    gen_server:cast(?SERVER, {unregister, ClientId, self()}).

%%------------------------------------------------------------------------------
%% @doc
%% Get statistics of client manager.
%%
%% @end
%%------------------------------------------------------------------------------
getstats() ->
    [{Name, emqttd_broker:getstat(Name)} || 
        Name <- ['clients/count', 'clients/max']].

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    TabId = ets:new(?CLIENT_TABLE, [set,
                                    named_table,
                                    public,
                                    {write_concurrency, true}]),
    {ok, #state{tab = TabId}}.

handle_call(Req, _From, State) ->
    lager:error("unexpected request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({register, ClientId, Pid}, State=#state{tab = Tab}) ->
    case registerd(Tab, {ClientId, Pid}) of
        true -> 
            ignore;
        false -> 
            ets:insert(Tab, {ClientId, Pid, erlang:monitor(process, Pid)})
    end,
    {noreply, setstats(State)};

handle_cast({monitor, ClientId, Pid}, State = #state{tab = Tab}) ->
    case ets:update_element(Tab, ClientId, {3, erlang:monitor(process, Pid)}) of
        true -> ok;
        false -> lager:error("failed to monitor clientId '~s' with pid ~p", [ClientId, Pid]) 
    end,
    {noreply, setstats(State)};

handle_cast({unregister, ClientId, Pid}, State) ->
	case ets:lookup(?CLIENT_TABLE, ClientId) of
	[{_, Pid, MRef}] ->
		erlang:demonitor(MRef, [flush]),
		ets:delete(?CLIENT_TABLE, ClientId);
	[_] -> 
		ignore;
	[] ->
		lager:error("cannot find clientId '~s' with ~p", [ClientId, Pid])
	end,
	{noreply, setstats(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(?CLIENT_TABLE, {'_', DownPid, MRef}),
    {noreply, setstats(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
registerd(Tab, {ClientId, Pid}) ->
	case ets:lookup(Tab, ClientId) of
        [{_, Pid, _}] ->
			lager:error("clientId '~s' has been registered with ~p", [ClientId, Pid]),
            true;
		[{_, OldPid, MRef}] ->
			lager:error("clientId '~s' is duplicated: pid=~p, oldpid=~p", [ClientId, Pid, OldPid]),
			OldPid ! {stop, duplicate_id, Pid},
			erlang:demonitor(MRef),
            false;
		[] -> 
            false
	end.

setstats(State) ->
    emqttd_broker:setstats('clients/count',
                           'clients/max',
                           ets:info(?CLIENT_TABLE, size)), State.


