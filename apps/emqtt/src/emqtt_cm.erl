%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
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

%client manager
-module(emqtt_cm).

-author('feng@emqtt.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TABLE, emqtt_client).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([lookup/1, register/2, unregister/2]).

-export([stats/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {max = 0}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%
%% @doc lookup client pid with clientId.
%%
-spec lookup(ClientId :: binary()) -> pid() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
	case ets:lookup(emqtt_client, ClientId) of
	[{_, Pid, _}] -> Pid;
	[] -> undefined
	end.

%%
%% @doc register clientId with pid.
%%
-spec register(ClientId :: binary(), Pid :: pid()) -> ok.
register(ClientId, Pid) when is_binary(ClientId), is_pid(Pid) ->
	gen_server:call(?SERVER, {register, ClientId, Pid}).

%%
%% @doc unregister clientId with pid.
%%
-spec unregister(ClientId :: binary(), Pid :: pid()) -> ok.
unregister(ClientId, Pid) when is_binary(ClientId), is_pid(Pid) ->
	gen_server:cast(?SERVER, {unregister, ClientId, Pid}).

stats() ->
    gen_server:call(?SERVER, stats).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
	ets:new(?TABLE, [set, named_table, protected]),
    {ok, #state{}}.

handle_call({register, ClientId, Pid}, _From, State) ->
	case ets:lookup(?TABLE, ClientId) of
        [{_, Pid, _}] ->
			lager:error("clientId '~s' has been registered with ~p", [ClientId, Pid]),
            ignore;
		[{_, OldPid, MRef}] ->
			OldPid ! {stop, duplicate_id, Pid},
			erlang:demonitor(MRef),
            insert(ClientId, Pid);
		[] -> 
            insert(ClientId, Pid)
	end,
	{reply, ok, set_max(State)};

handle_call(stats, _From, State = #state{max = Max}) ->
    Stats = [{'clients/total', ets:info(?TABLE, size)},
             {'clients/max', Max}],
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({unregister, ClientId, Pid}, State) ->
	case ets:lookup(?TABLE, ClientId) of
	[{_, Pid, MRef}] ->
		erlang:demonitor(MRef),
		ets:delete(?TABLE, ClientId);
	[_] -> 
		ignore;
	[] ->
		lager:error("cannot find clientId '~s' with ~p", [ClientId, Pid])
	end,
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(?TABLE, {{'_', DownPid, MRef}}),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

insert(ClientId, Pid) ->
    ets:insert(?TABLE, {ClientId, Pid, erlang:monitor(process, Pid)}).

set_max(State = #state{max = Max}) ->
    Total = ets:info(?TABLE, size),
    if
        Total > Max -> State#state{max = Total};
        true -> State
    end.
