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
    start_link/1,
    stop/1
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
    ping/2,
    ack/4
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

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec connect(binary(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()) ->
    ok | {error, term()}.
connect(#{topic_filter := MQTopicFilter} = MQ, SubscriberRef, ClientId) ->
    ?tp(warning, mq_consumer_connect, #{
        mq_topic_filter => MQTopicFilter, subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case find_consumer(MQ, _Retries = 2) of
        {ok, ConsumerRef} ->
            ?tp(warning, mq_consumer_find_consumer_success, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId,
                consumer_ref => ConsumerRef
            }),
            send_to_consumer_server(ConsumerRef, #mq_server_connect{
                subscriber_ref = SubscriberRef, client_id = ClientId
            });
        queue_removed ->
            ?tp(warning, mq_consumer_find_consumer_queue_removed, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId
            }),
            {error, queue_removed};
        not_found ->
            ?tp(warning, mq_consumer_find_consumer_not_found, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId
            }),
            {error, consumer_not_found}
    end.

-spec disconnect(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
disconnect(Pid, SubscriberRef) ->
    send_to_consumer_server(Pid, #mq_server_disconnect{subscriber_ref = SubscriberRef}).

-spec ack(
    emqx_mq_types:consumer_ref(),
    emqx_mq_types:subscriber_ref(),
    emqx_mq_types:message_id(),
    emqx_mq_types:ack()
) -> ok.
ack(Pid, SubscriberRef, MessageId, Ack) ->
    send_to_consumer_server(Pid, #mq_server_ack{
        subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack
    }).

-spec ping(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
ping(Pid, SubscriberRef) ->
    send_to_consumer_server(Pid, #mq_server_ping{subscriber_ref = SubscriberRef}).

-spec stop(emqx_mq_types:consumer_ref()) -> ok.
stop(Pid) ->
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
    ?tp(warning, mq_consumer_init, #{mq_topic_filter => MQTopicFilter, claim_res => ClaimRes}),
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
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(Request, State) ->
    ?tp(warning, mq_consumer_cast_unknown_request, #{request => Request}),
    {noreply, State}.

handle_info(#info_to_mq_server{message = Message}, State) ->
    handle_mq_server_info(Message, State);
handle_info(#persist_consumer_data{}, State) ->
    handle_persist_consumer_data(State);
handle_info(Request, #state{mq = #{topic_filter := MQTopicFilter}} = State0) ->
    % ?tp(warning, mq_consumer_handle_info, #{request => Request, mq_topic => MQTopicFilter}),
    case handle_ds_info(Request, State0) of
        ignore ->
            ?tp(warning, mq_consumer_unknown_info, #{request => Request, mq_topic => MQTopicFilter}),
            {noreply, State0};
        {ok, State} ->
            {noreply, State}
    end.

terminate(_Reason, #state{} = State) ->
    handle_shutdown(State).

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

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
    ?tp(warning, mq_consumer_handle_persist_streams_data, #{
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
    ?tp(warning, mq_consumer_shutdown, #{
        mq_topic_filter => MQTopicFilter,
        persist_res => PersistRes,
        drop_leadership_res => DropLeadershipRes
    }),
    ok.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

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
                    ?tp(warning, mq_consumer_find_consumer_error, #{
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
