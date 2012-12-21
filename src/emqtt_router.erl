-module(emqtt_router).

-include("emqtt.hrl").
-include("emqtt_frame.hrl").

-export([start_link/0]).

-export([route/1,
		route/2,
		insert/1,
		delete/1]).

-behaviour(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

binary(S) when is_list(S) ->
	list_to_binary(S);

binary(B) when is_binary(B) ->
	B.

route(#mqtt_msg{topic=Topic}=Msg) when is_record(Msg, mqtt_msg) ->
	error_logger:info_msg("route msg: ~p~n", [Msg]),
	[ Pid ! {route, Msg} || #subscriber{pid=Pid} <- ets:lookup(subscriber, binary(Topic)) ].

route(Topic, Msg) ->
	[ Pid ! {route, Msg} || #subscriber{pid=Pid} <- ets:lookup(subscriber, Topic) ].

insert(Sub) when is_record(Sub, subscriber) ->
	gen_server:call(?MODULE, {insert, Sub}).

delete(Sub) when is_record(Sub, subscriber) ->
	gen_server:cast(?MODULE, {delete, Sub}).

init([]) ->
	Res = ets:new(subscriber, [bag, protected, named_table, {keypos, 2}]),
	error_logger:info_msg("emqtt_router is started: ~p~n", [Res]),
	{ok, #state{}}.

handle_call({insert, Sub}, _From, State) ->
	ets:insert(subscriber, Sub),
	{reply, ok, State};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({delete, Sub}, State) ->
	ets:delete_object(subscriber, Sub),
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, _State, _Extra) ->
	ok.


