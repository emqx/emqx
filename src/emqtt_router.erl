-module(emqtt_router).

-include("emqtt.hrl").

-include("emqtt_frame.hrl").

-include_lib("stdlib/include/qlc.hrl").

-export([start_link/0]).

-export([topics/1,
		subscribe/2,
		unsubscribe/2,
		publish/2,
		route/2,
		down/1]).

-behaviour(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {}).

start_link() ->
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

topics(direct) ->
	mnesia:dirty_all_keys(direct_topic);

topics(wildcard) ->
	mnesia:dirty_all_keys(wildcard_topic).

subscribe({Topic, Qos}, Client) when is_pid(Client) ->
	gen_server2:call(?MODULE, {subscribe, {Topic, Qos}, Client}).

unsubscribe(Topic, Client) when is_binary(Topic) and is_pid(Client) ->
	gen_server2:cast(?MODULE, {unsubscribe, Topic, Client}).

%publish to cluster node.
publish(Topic, Msg) when is_binary(Topic) and is_record(Msg, mqtt_msg) ->
	lists:foreach(fun(#topic{name=Name, node=Node}) ->
		case Node == node() of
		true -> route(Name, Msg);
		false -> rpc:call(Node, ?MODULE, route, [Name, Msg])
		end
	end, match(Topic)).

%route locally, should only be called by publish
route(Topic, Msg) ->
	[Client ! {route, Msg} || #subscriber{client=Client} <- ets:lookup(subscriber, Topic)].

match(Topic) when is_binary(Topic)  ->
	DirectMatches = mnesia:dirty_read(direct_topic, Topic),
	TopicWords = emqtt_topic:words(Topic),
	WildcardQuery = qlc:q([T || T = #topic{words=Words}
								<- mnesia:table(wildcard_topic), 
									emqtt_topic:match(TopicWords, Words)]), %
	
	{atomic, WildcardMatches} = mnesia:transaction(fun() -> qlc:e(WildcardQuery) end),
	%mnesia:async_dirty(fun qlc:e/1, WildcardQuery),
	%?INFO("~p", [WildcardMatches]),
	DirectMatches ++ WildcardMatches.

down(Client) when is_pid(Client) ->
	gen_server2:cast(?MODULE, {down, Client}).

init([]) ->
	mnesia:create_table(direct_topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(direct_topic, node(), ram_copies),
	mnesia:create_table(wildcard_topic, [
		{type, bag},
		{index, [#topic.words]},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(wildcard_topic, node(), ram_copies),
	ets:new(subscriber, [bag, named_table, {keypos, 2}]),
	?INFO_MSG("emqtt_router is started."),
	{ok, #state{}}.

handle_call({subscribe, {Name, Qos}, Client}, _From, State) ->
	Topic = #topic{name=Name, node=node(), words=emqtt_topic:words(Name)},
	case emqtt_topic:type(Topic) of
	direct -> 
		ok = mnesia:dirty_write(direct_topic, Topic);
	wildcard -> 
		ok = mnesia:dirty_write(wildcard_topic, Topic)
	end,
	ets:insert(subscriber, #subscriber{topic=Name, qos=Qos, client=Client}),
	emqtt_retained:send(Name, Client),
	{reply, ok, State};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({unsubscribe, Topic, Client}, State) ->
	ets:match_delete(subscriber, {subscriber, Topic, '_', Client}),
	try_remove_topic(Topic),
	{noreply, State};

handle_cast({down, Client}, State) ->
	case ets:match_object(subscriber, {subscriber, '_', '_', Client}) of
	[] -> 
		ignore;
	Subs -> 
		[ets:delete_object(subscriber, Sub) || Sub <- Subs],
		[try_remove_topic(Topic) || #subscriber{topic=Topic} <- Subs]
	end,
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.


handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ------------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------------
try_remove_topic(Name) ->
	case ets:member(subscriber, Name) of
	false -> 
		Topic = emqtt_topic:new(Name),
		case emqtt_topic:type(Topic) of
		direct ->
			mnesia:dirty_delete_object(direct_topic, Topic);
		wildcard -> 
			mnesia:dirty_delete_object(wildcard_topic, Topic)
		end;
	true -> ok
	end.

