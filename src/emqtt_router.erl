-module(emqtt_router).

-include("emqtt.hrl").

-include("emqtt_frame.hrl").

-export([start_link/0]).

-export([subscribe/2,
		unsubscribe/2,
		publish/2]).

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

publish(Topic, Msg) when is_binary(Topic) and is_record(Msg, mqtt_msg) ->
	[	
		[Client ! {route, Msg} || #subscriber{client=Client} <- ets:lookup(subscriber, Path)]
	|| #topic{path=Path} <- match(Topic)].


match(Topic) when is_binary(Topic)  ->
	Words = topic_split(Topic), 
	DirectMatches = mnesia:dirty_read(direct_topic, Words),
	WildcardMatches = lists:append([
		mnesia:dirty_read(wildcard_topic, Key)	|| 
			Key <- mnesia:dirty_all_keys(wildcard_topic),
				topic_match(Words, Key)
	]),
	DirectMatches ++ WildcardMatches.

init([]) ->
	mnesia:create_table(
		direct_topic, [
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(direct_topic, node(), ram_copies),
	mnesia:create_table(
		wildcard_topic, [
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(wildcard_topic, node(), ram_copies),
	ets:new(subscriber, [bag, named_table, {keypos, 2}]),
	?INFO_MSG("emqtt_router is started."),
	{ok, #state{}}.

handle_call({subscribe, Topic, Client}, _From, State) ->
	Words = topic_split(Topic),
	case topic_type(Words) of
	direct -> 
		ok = mnesia:dirty_write(direct_topic, #topic{words=Words, path=Topic});
	wildcard -> 
		ok = mnesia:dirty_write(wildcard_topic, #topic{words=Words, path=Topic})
	end,
	Ref = erlang:monitor(process, Client),
	ets:insert(subscriber, #subscriber{topic=Topic, client=Client, monref=Ref}),
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

code_change(_OldVsn, _State, _Extra) ->
	ok.

%--------------------------------------
% internal functions
%--------------------------------------

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
	

