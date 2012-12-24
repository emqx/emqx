-module(emqtt_router).

-include("emqtt.hrl").

-include("emqtt_frame.hrl").

-include_lib("stdlib/include/qlc.hrl").

-export([start_link/0]).

-export([subscribe/2,
		unsubscribe/2,
		publish/2,
		route/2]).

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

subscribe(Topic, Client) when is_binary(Topic) and is_pid(Client) ->
	gen_server2:call(?MODULE, {subscribe, Topic, Client}).

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
	TopicWords = topic_split(Topic),
	WildcardQuery = qlc:q([T || T = #topic{words=Words}
								<- mnesia:table(wildcard_topic), 
									topic_match(TopicWords, Words)]), %
	
	{atomic, WildcardMatches} = mnesia:transaction(fun() -> qlc:e(WildcardQuery) end), %mnesia:async_dirty(fun qlc:e/1, WildcardQuery),
	?INFO("~p", [WildcardMatches]),
	DirectMatches ++ WildcardMatches.

init([]) ->
	mnesia:create_table(direct_topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(direct_topic, node(), ram_copies),
	mnesia:create_table(wildcard_topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(wildcard_topic, node(), ram_copies),
	ets:new(subscriber, [bag, named_table, {keypos, 2}]),
	?INFO_MSG("emqtt_router is started."),
	{ok, #state{}}.

handle_call({subscribe, Name, Client}, _From, State) ->
	Topic = #topic{name=Name, node=node(), words=topic_split(Name)},
	case topic_type(Topic) of
	direct -> 
		ok = mnesia:dirty_write(direct_topic, Topic);
	wildcard -> 
		ok = mnesia:dirty_write(wildcard_topic, Topic)
	end,
	Ref = erlang:monitor(process, Client),
	ets:insert(subscriber, #subscriber{topic=Name, client=Client, monref=Ref}),
	emqtt_retained:send(Name, Client),
	{reply, ok, State};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({unsubscribe, Topic, Client}, State) ->
	ets:match_delete(subscriber, #subscriber{topic=Topic, client=Client}),
	%TODO: how to remove topic
	%
	%Words = topic_split(Topic),
	%case topic_type(Words) of
	%direct ->
	%	mnesia:dirty_delete(direct_topic, #topic{words=Words, path=Topic});
	%wildcard -> 
	%	mnesia:direct_delete(wildcard_topic, #topic{words=Words, path=Topic})
	%end,
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State) ->
	ets:match_delete(subscriber, #subscriber{monref=MonitorRef}),
	{noreply, State};

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%--------------------------------------
% internal functions
%--------------------------------------
topic_type(#topic{words=Words}) ->
	topic_type(Words);
topic_type([]) ->
	direct;
topic_type([<<"#">>]) ->
	wildcard;
topic_type([<<"+">>|_T]) ->
	wildcard;
topic_type([_|T]) ->
	topic_type(T).

topic_match([], []) ->
	true;
topic_match([H|T1], [H|T2]) ->
	topic_match(T1, T2);
topic_match([_H|T1], [<<"+">>|T2]) ->
	topic_match(T1, T2);
topic_match(_, [<<"#">>]) ->
	true;
topic_match([_H1|_], [_H2|_]) ->
	false;
topic_match([], [_H|_T2]) ->
	false.
	
topic_split(S) ->
	binary:split(S, [<<"/">>], [global]).

