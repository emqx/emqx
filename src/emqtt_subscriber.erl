-module(emqtt_subscriber).

-include("emqtt.hrl").

-export([start_link/0]).

-behaviour(gen_server).

-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-record(state,{}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	ets:new(subscriber, [bag, protected, {keypos, 2}]),
	{ok, #state{}}.

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg},  State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, _State, _Extra) ->
	ok.




