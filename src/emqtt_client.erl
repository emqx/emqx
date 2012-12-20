-module(emqtt_client).

-behaviour(gen_server2).

-export([start_link/0, go/2]).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
        code_change/3,
		terminate/2]).

-include("emqtt.hrl").

go(Pid, Sock) ->
	gen_server2:call(Pid, {go, Sock}).

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

init([]) ->
    {ok, undefined, hibernate, {backoff, 1000, 1000, 10000}}.

handle_call({go, Sock}, _From, State) ->
	error_logger:info_msg("go.... sock: ~p", [Sock]),
	{reply, ok, State}.

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
