%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer).

-moduledoc """
The module represents a consumer of the Message Queue.

A consumer exclusively serves a single Message Queue, so for a given
Message Queue, there is only one or zero consumers.

Consumer's responsibilities:
* Consume messages from the Message Queue topic streams.
* Dispatch messages to the consumer clients (channels).
* Receive message acknowledgements from the consumer clients.
* Track consumption progeress of the topic streams.
""".

-include("emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start_link/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    connect/3,
    disconnect/2,
    ack/4,
    ping/2,
    stop/1,
    info/2
]).

%% RPC targets
-export([
    connect_v1/3,
    disconnect_v1/2,
    ack_v1/4,
    ping_v1/2,
    stop_v1/1
]).

-record(state, {
    mq :: emqx_mq_types:mq(),
    streams :: emqx_mq_consumer_streams:t(),
    server :: emqx_mq_consumer_server:t()
}).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(persist_consumer_data, {}).
-record(get_state_info, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec connect(binary(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()) ->
    ok | {error, term()}.
connect(#{topic_filter := MQTopicFilter} = MQ, SubscriberRef, ClientId) ->
    ?tp_debug(mq_consumer_connect, #{
        mq_topic_filter => MQTopicFilter, subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case find_consumer(MQ, _Retries = 2) of
        {ok, ConsumerRef} ->
            ?tp_debug(mq_consumer_find_consumer_success, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId,
                consumer_ref => ConsumerRef
            }),
            do_connect(ConsumerRef, SubscriberRef, ClientId);
        queue_removed ->
            ?tp_debug(mq_consumer_find_consumer_queue_removed, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId
            }),
            {error, queue_removed};
        not_found ->
            ?tp_debug(mq_consumer_find_consumer_not_found, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId
            }),
            {error, consumer_not_found}
    end.

-spec disconnect(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
disconnect(ConsumerRef, SubscriberRef) when node(ConsumerRef) =:= node() ->
    disconnect_v1(ConsumerRef, SubscriberRef);
disconnect(ConsumerRef, SubscriberRef) ->
    emqx_mq_consumer_proto_v1:mq_server_disconnect(node(ConsumerRef), ConsumerRef, SubscriberRef).

-spec ack(
    emqx_mq_types:consumer_ref(),
    emqx_mq_types:subscriber_ref(),
    emqx_mq_types:message_id(),
    emqx_mq_types:ack()
) -> ok.
ack(ConsumerRef, SubscriberRef, MessageId, Ack) when node(ConsumerRef) =:= node() ->
    ack_v1(ConsumerRef, SubscriberRef, MessageId, Ack);
ack(ConsumerRef, SubscriberRef, MessageId, Ack) ->
    emqx_mq_consumer_proto_v1:mq_server_ack(
        node(ConsumerRef), ConsumerRef, SubscriberRef, MessageId, Ack
    ).

-spec ping(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
ping(ConsumerRef, SubscriberRef) when node(ConsumerRef) =:= node() ->
    ping_v1(ConsumerRef, SubscriberRef);
ping(ConsumerRef, SubscriberRef) ->
    emqx_mq_consumer_proto_v1:mq_server_ping(node(ConsumerRef), ConsumerRef, SubscriberRef).

-spec stop(emqx_mq_types:consumer_ref()) -> ok.
stop(ConsumerRef) when node(ConsumerRef) =:= node() ->
    stop_v1(ConsumerRef);
stop(ConsumerRef) ->
    emqx_mq_consumer_proto_v1:mq_server_stop(node(ConsumerRef), ConsumerRef).

-spec info(emqx_mq_types:consumer_ref(), timeout()) -> emqx_types:infos().
info(ConsumerRef, Timeout) ->
    gen_server:call(ConsumerRef, #get_state_info{}, Timeout).

%%--------------------------------------------------------------------
%% RPC targets
%%--------------------------------------------------------------------

-spec connect_v1(
    emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()
) -> ok.
connect_v1(ConsumerRef, SubscriberRef, ClientId) ->
    send_to_consumer_server(ConsumerRef, #mq_server_connect{
        subscriber_ref = SubscriberRef, client_id = ClientId
    }).

-spec disconnect_v1(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
disconnect_v1(ConsumerRef, SubscriberRef) ->
    ?tp_debug(mq_consumer_disconnect_v1, #{
        consumer_ref => ConsumerRef, subscriber_ref => SubscriberRef
    }),
    send_to_consumer_server(ConsumerRef, #mq_server_disconnect{subscriber_ref = SubscriberRef}).

-spec ack_v1(
    emqx_mq_types:consumer_ref(),
    emqx_mq_types:subscriber_ref(),
    emqx_mq_types:message_id(),
    emqx_mq_types:ack()
) -> ok.
ack_v1(Pid, SubscriberRef, MessageId, Ack) ->
    send_to_consumer_server(Pid, #mq_server_ack{
        subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack
    }).

-spec ping_v1(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
ping_v1(Pid, SubscriberRef) ->
    send_to_consumer_server(Pid, #mq_server_ping{subscriber_ref = SubscriberRef}).

-spec stop_v1(emqx_mq_types:consumer_ref()) -> ok.
stop_v1(Pid) ->
    try
        gen_server:call(Pid, stop, infinity)
    catch
        exit:{noproc, _} ->
            ok
    end.

%%--------------------------------------------------------------------
%% Gen Server Callbacks
%%--------------------------------------------------------------------

init([#{topic_filter := MQTopicFilter} = MQ]) ->
    erlang:process_flag(trap_exit, true),
    ClaimRes = emqx_mq_consumer_db:claim_leadership(MQ, self_consumer_ref(), now_ms()),
    ?tp_debug(mq_consumer_init, #{mq_topic_filter => MQTopicFilter, claim_res => ClaimRes}),
    case ClaimRes of
        {ok, ConsumerData} ->
            erlang:send_after(
                ?DEFAULT_CONSUMER_PERSISTENCE_INTERVAL, self(), #persist_consumer_data{}
            ),
            Progress = maps:get(progress, ConsumerData, #{}),
            Streams = emqx_mq_consumer_streams:new(MQ, Progress),
            Server = emqx_mq_consumer_server:new(MQ),
            {ok, #state{
                mq = MQ,
                streams = Streams,
                server = Server
            }};
        {error, _, _} = Error ->
            {stop, Error}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(#get_state_info{}, _From, State) ->
    {reply, handle_get_state_info(State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(Request, State) ->
    ?tp_debug(mq_consumer_cast_unknown_request, #{request => Request}),
    {noreply, State}.

handle_info(#info_to_mq_server{message = Message}, State) ->
    handle_mq_server_info(Message, State);
handle_info(#persist_consumer_data{}, State) ->
    handle_persist_consumer_data(State);
handle_info(Request, #state{mq = #{topic_filter := MQTopicFilter}} = State0) ->
    ?tp_debug(mq_consumer_handle_info, #{request => Request, mq_topic_filter => MQTopicFilter}),
    case handle_ds_info(Request, State0) of
        ignore ->
            ?tp_debug(mq_consumer_unknown_info, #{request => Request, mq_topic => MQTopicFilter}),
            {noreply, State0};
        {ok, State} ->
            {noreply, State}
    end.

terminate(_Reason, #state{} = State) ->
    handle_shutdown(State).

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

handle_get_state_info(#state{
    mq = #{topic_filter := TopicFilter} = _MQ, streams = Streams, server = Server
}) ->
    #{
        mq_topic_filter => TopicFilter,
        streams => emqx_mq_consumer_streams:info(Streams),
        server => emqx_mq_consumer_server:info(Server)
    }.

handle_mq_server_info(Message, #state{server = Server0} = State) ->
    {ok, Events, Server} = emqx_mq_consumer_server:handle_info(Server0, Message),
    handle_events(Events, State#state{server = Server}).

handle_events([], State) ->
    {noreply, State};
handle_events([{ds_ack, MessageId} | Rest], #state{streams = Streams0} = State) ->
    Streams = emqx_mq_consumer_streams:handle_ack(Streams0, MessageId),
    handle_events(Rest, State#state{streams = Streams});
handle_events([shutdown | _Rest], State) ->
    {stop, normal, State}.

handle_ds_info(Request, #state{streams = Streams0, server = Server0} = State) ->
    case emqx_mq_consumer_streams:handle_ds_info(Streams0, Request) of
        {ok, Messages, Streams} ->
            Server = emqx_mq_consumer_server:handle_messages(Server0, Messages),
            {ok, State#state{streams = Streams, server = Server}};
        ignore ->
            ignore
    end.

handle_persist_consumer_data(State) ->
    case persist_consumer_data(State) of
        ok ->
            erlang:send_after(
                ?DEFAULT_CONSUMER_PERSISTENCE_INTERVAL, self(), #persist_consumer_data{}
            ),
            {noreply, State};
        Error ->
            ?tp(error, mq_consumer_persist_consumer_data_error, #{error => Error}),
            {stop, Error, State}
    end.

persist_consumer_data(#state{mq = #{topic_filter := MQTopicFilter} = MQ, streams = Streams}) ->
    PersistData = #{
        progress => emqx_mq_consumer_streams:progress(Streams)
    },
    ?tp_debug(mq_consumer_handle_persist_streams_data, #{
        mq_topic_filter => MQTopicFilter,
        streams_info => emqx_mq_consumer_streams:info(Streams),
        persist_streams_data => PersistData
    }),
    emqx_mq_consumer_db:update_consumer_data(
        MQ,
        self_consumer_ref(),
        PersistData,
        now_ms()
    ).

handle_shutdown(#state{mq = #{topic_filter := MQTopicFilter} = MQ} = State) ->
    PersistRes = persist_consumer_data(State),
    DropLeadershipRes = emqx_mq_consumer_db:drop_leadership(MQ),
    ?tp_debug(mq_consumer_shutdown, #{
        mq_topic_filter => MQTopicFilter,
        persist_res => PersistRes,
        drop_leadership_res => DropLeadershipRes
    }),
    ok.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

do_connect(ConsumerRef, SubscriberRef, ClientId) when node(ConsumerRef) =:= node() ->
    connect_v1(ConsumerRef, SubscriberRef, ClientId);
do_connect(ConsumerRef, SubscriberRef, ClientId) ->
    emqx_mq_consumer_proto_v1:mq_server_connect(
        node(ConsumerRef), ConsumerRef, SubscriberRef, ClientId
    ).

find_consumer(#{topic_filter := MQTopicFilter} = _MQ, 0) ->
    ?tp(error, mq_consumer_find_consumer_error, #{
        error => retries_exhausted, mq_topic_filter => MQTopicFilter
    }),
    not_found;
find_consumer(#{topic_filter := MQTopicFilter} = MQ, Retries) ->
    case emqx_mq_consumer_db:find_consumer(MQ, now_ms()) of
        {ok, ConsumerRef} ->
            {ok, ConsumerRef};
        queue_removed ->
            queue_removed;
        not_found ->
            case emqx_mq_sup:start_consumer(_ChildId = MQTopicFilter, [MQ]) of
                {ok, ConsumerRef} ->
                    {ok, ConsumerRef};
                {error, _} = Error ->
                    ?tp_debug(mq_consumer_find_consumer_error, #{
                        error => Error, retries => Retries, mq_topic_filter => MQTopicFilter
                    }),
                    find_consumer(MQ, Retries - 1)
            end
    end.

now_ms() ->
    erlang:system_time(millisecond).

self_consumer_ref() ->
    self().

send_to_consumer_server(Pid, InfoMsg) ->
    _ = erlang:send(Pid, #info_to_mq_server{message = InfoMsg}),
    ok.
