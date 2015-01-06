%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
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

-author('feng@slimchat.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TAB, emqtt_client).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([lookup/1, create/2, destroy/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec lookup(ClientId :: binary()) -> pid() | undefined.
lookup(ClientId) ->
	case ets:lookup(emqtt_client, ClientId) of
	[{_, Pid, _}] -> Pid;
	[] -> undefined
	end.

-spec create(ClientId :: binary(), Pid :: pid()) -> ok.
create(ClientId, Pid) ->
	gen_server:call(?SERVER, {create, ClientId, Pid}).

-spec destroy(ClientId :: binary(), Pid :: pid()) -> ok.
destroy(ClientId, Pid) when is_binary(ClientId) ->
	gen_server:cast(?SERVER, {destroy, ClientId, Pid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
	%on one node
	ets:new(?TAB, [set, named_table, protected]),
    {ok, Args}.

handle_call({create, ClientId, Pid}, _From, State) ->
	case ets:lookup(?TAB, ClientId) of
        [{_, Pid, _}] ->
			lager:error("client '~s' has been registered with ~p", [ClientId, Pid]),
            ignore;
		[{_, OldPid, MRef}] ->
			OldPid ! {stop, duplicate_id},
			erlang:demonitor(MRef),
            ets:insert(emqtt_client, {ClientId, Pid, erlang:monitor(process, Pid)});
		[] -> 
            ets:insert(emqtt_client, {ClientId, Pid, erlang:monitor(process, Pid)})
	end,
	{reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({destroy, ClientId, Pid}, State) when is_binary(ClientId) ->
	case ets:lookup(?TAB, ClientId) of
	[{_, Pid, MRef}] ->
		erlang:demonitor(MRef),
		ets:delete(?TAB, ClientId);
	[_] ->
		ignore;
	[] ->
		lager:error("cannot find client '~s' with ~p", [ClientId, Pid])
	end,
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(emqtt_client, {{'_', DownPid, MRef}}),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


