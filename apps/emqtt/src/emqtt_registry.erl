
%%TODO: SHOULD BE REPLACED BY emqtt_cm.erl......

-module(emqtt_registry).

-include("emqtt.hrl").

-include("emqtt_log.hrl").

-export([start_link/0, 
		size/0,
		register/2,
		unregister/1]).

-behaviour(gen_server).

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {}).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

size() ->
	ets:info(client, size).

register(ClientId, Pid) ->
    gen_server:cast(?SERVER, {register, ClientId, Pid}).

unregister(ClientId) ->
    gen_server:cast(?SERVER, {unregister, ClientId}).

%%----------------------------------------------------------------------------

init([]) ->
	ets:new(client, [set, protected, named_table]),
	?INFO("~p is started.", [?MODULE]),
    {ok, #state{}}. % clientid -> {pid, monitor}

%%--------------------------------------------------------------------------
handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast({register, ClientId, Pid}, State) ->
	case ets:lookup(client, ClientId) of
	[{_, {OldPid, MRef}}] ->
		catch gen_server:call(OldPid, duplicate_id),
		erlang:demonitor(MRef);
	[] ->
		ignore
	end,
	ets:insert(client, {ClientId, {Pid, erlang:monitor(process, Pid)}}),
    {noreply, State};

handle_cast({unregister, ClientId}, State) ->
	case ets:lookup(client, ClientId) of
	[{_, {_Pid, MRef}}] ->
		erlang:demonitor(MRef),
		ets:delete(client, ClientId);
	[] ->
		ignore
	end,
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(client, {'_', {DownPid, MRef}}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

