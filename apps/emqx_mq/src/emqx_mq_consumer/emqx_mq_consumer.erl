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

-include("../emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start_link/1,
    child_spec/2
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
    find/1,
    disconnect/2,
    ack/4,
    ping/2,
    stop/1,
    inspect/2
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
    server :: emqx_mq_consumer_server:t(),
    consumer_state :: emqx_mq_state_storage:consumer_state()
}).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(persist_consumer_data, {}).
-record(inspect, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(emqx_mq_types:mq_topic()) -> gen_server:start_ret().
start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec child_spec(emqx_mq_types:consumer_sup_id(), [emqx_mq_types:mq()]) -> supervisor:child_spec().
child_spec(Id, Args) ->
    #{
        id => Id,
        start => {emqx_mq_consumer, start_link, Args},
        restart => temporary,
        shutdown => 5000
    }.

-spec find(emqx_mq_types:mq_id()) -> {ok, emqx_mq_types:consumer_ref()} | not_found.
find(Id) ->
    case global:whereis_name(global_name(Id)) of
        ConsumerRef when is_pid(ConsumerRef) ->
            {ok, ConsumerRef};
        undefined ->
            not_found
    end.

-spec connect(emqx_mq_types:mq(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()) ->
    ok | {error, term()}.
connect(#{topic_filter := _MQTopicFilter} = MQ, SubscriberRef, ClientId) ->
    ?tp_debug(mq_consumer_connect, #{
        mq_topic_filter => _MQTopicFilter, subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case find_or_start(MQ) of
        {ok, ConsumerRef} ->
            ?tp_debug(mq_consumer_find_consumer_success, #{
                mq_topic_filter => _MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId,
                consumer_ref => ConsumerRef
            }),
            do_connect(ConsumerRef, SubscriberRef, ClientId);
        not_found ->
            ?tp_debug(mq_consumer_find_consumer_not_found, #{
                mq_topic_filter => _MQTopicFilter,
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

-spec inspect(emqx_mq_types:consumer_ref(), timeout()) -> emqx_types:infos().
inspect(ConsumerRef, Timeout) ->
    gen_server:call(ConsumerRef, #inspect{}, Timeout).

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

init([#{topic_filter := _MQTopicFilter, consumer_persistence_interval := PersistenceInterval} = MQ]) ->
    erlang:process_flag(trap_exit, true),
    case try_register_consumer(MQ) of
        ok ->
            ConsumerStateRes = emqx_mq_state_storage:open_consumer_state(MQ),
            ?tp_debug(mq_consumer_init, #{mq_topic_filter => _MQTopicFilter}),
            case ConsumerStateRes of
                {ok, ConsumerState} ->
                    erlang:send_after(
                        PersistenceInterval, self(), #persist_consumer_data{}
                    ),
                    Progress = emqx_mq_state_storage:get_shards_progress(ConsumerState),
                    Streams = emqx_mq_consumer_streams:new(MQ, Progress),
                    Server = emqx_mq_consumer_server:new(MQ),
                    {ok, #state{
                        mq = MQ,
                        streams = Streams,
                        server = Server,
                        consumer_state = ConsumerState
                    }};
                {error, _} = Error ->
                    {stop, Error}
            end;
        {error, already_registered} ->
            {stop, already_registered}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(#inspect{}, _From, State) ->
    {reply, handle_inspect(State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    ?tp_debug(mq_consumer_cast_unknown_request, #{request => _Request}),
    {noreply, State}.

handle_info(#info_to_mq_server{message = Message}, State) ->
    handle_mq_server_info(Message, State);
handle_info(#persist_consumer_data{}, State) ->
    handle_persist_consumer_data(State);
handle_info(Request, #state{mq = #{topic_filter := _MQTopicFilter}} = State0) ->
    ?tp_debug(mq_consumer_handle_info, #{request => Request, mq_topic_filter => _MQTopicFilter}),
    case handle_ds_info(Request, State0) of
        ignore ->
            ?tp_debug(mq_consumer_unknown_info, #{
                request => Request, mq_topic_filter => _MQTopicFilter
            }),
            {noreply, State0};
        {ok, State} ->
            {noreply, State}
    end.

terminate(_Reason, #state{} = State) ->
    handle_shutdown(State).

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

handle_inspect(#state{
    mq = #{topic_filter := TopicFilter} = _MQ, streams = Streams, server = Server
}) ->
    #{
        mq_topic_filter => TopicFilter,
        streams => emqx_mq_consumer_streams:inspect(Streams),
        server => emqx_mq_consumer_server:inspect(Server)
    }.

handle_mq_server_info(Message, #state{server = Server0} = State) ->
    {ok, Events, Server} = emqx_mq_consumer_server:handle_info(Server0, Message),
    handle_events(Events, State#state{server = Server}).

handle_events([], State) ->
    {noreply, State};
handle_events(
    [{ds_ack, MessageId} | Rest],
    #state{streams = Streams0, consumer_state = ConsumerState0} = State
) ->
    Streams1 = emqx_mq_consumer_streams:handle_ack(Streams0, MessageId),
    {ProgressUpdates, Streams} = emqx_mq_consumer_streams:fetch_progress_updates(Streams1),
    ConsumerState = put_progress_updates(ConsumerState0, ProgressUpdates),
    handle_events(Rest, State#state{streams = Streams, consumer_state = ConsumerState});
handle_events([shutdown | _Rest], State) ->
    {stop, normal, State}.

handle_ds_info(
    Request, #state{streams = Streams0, server = Server0, consumer_state = ConsumerState0} = State
) ->
    case emqx_mq_consumer_streams:handle_ds_info(Streams0, Request) of
        {ok, Messages, Streams1} ->
            {ProgressUpdates, Streams} = emqx_mq_consumer_streams:fetch_progress_updates(Streams1),
            ConsumerState = put_progress_updates(ConsumerState0, ProgressUpdates),
            Server = emqx_mq_consumer_server:handle_messages(Server0, Messages),
            {ok, State#state{streams = Streams, server = Server, consumer_state = ConsumerState}};
        ignore ->
            ignore
    end.

handle_persist_consumer_data(
    #state{mq = #{consumer_persistence_interval := PersistenceInterval}} = State0
) ->
    case persist_consumer_data(_ClaimLeadership = true, State0) of
        {ok, State} ->
            erlang:send_after(
                PersistenceInterval, self(), #persist_consumer_data{}
            ),
            {noreply, State};
        Error ->
            ?tp(error, mq_consumer_persist_consumer_data_error, #{error => Error}),
            {stop, Error, State0}
    end.

persist_consumer_data(
    ClaimLeadership,
    #state{consumer_state = ConsumerState0, mq = #{topic_filter := _MQTopicFilter}} = State
) ->
    ?tp_debug(mq_consumer_handle_persist_consumer_state, #{
        mq_topic_filter => _MQTopicFilter
    }),
    case emqx_mq_state_storage:commit_consumer_state(ClaimLeadership, ConsumerState0) of
        {ok, ConsumerState} ->
            {ok, State#state{consumer_state = ConsumerState}};
        {error, _} = Error ->
            Error
    end.

handle_shutdown(#state{mq = #{topic_filter := _MQTopicFilter} = _MQ} = State) ->
    ?tp_debug(mq_consumer_shutdown, #{mq_topic_filter => _MQTopicFilter}),
    case persist_consumer_data(_ClaimLeadership = false, State) of
        {ok, _State} ->
            ok;
        {error, Reason} ->
            ?tp(error, mq_consumer_shutdown_error, #{
                mq_topic_filter => _MQTopicFilter,
                reason => Reason
            })
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

put_progress_updates(ConsumerState, ProgressUpdates) ->
    maps:fold(
        fun(Shard, Progress, ConsumerStateAcc) ->
            emqx_mq_state_storage:put_shard_progress(Shard, Progress, ConsumerStateAcc)
        end,
        ConsumerState,
        ProgressUpdates
    ).

do_connect(ConsumerRef, SubscriberRef, ClientId) when node(ConsumerRef) =:= node() ->
    connect_v1(ConsumerRef, SubscriberRef, ClientId);
do_connect(ConsumerRef, SubscriberRef, ClientId) ->
    emqx_mq_consumer_proto_v1:mq_server_connect(
        node(ConsumerRef), ConsumerRef, SubscriberRef, ClientId
    ).

find_or_start(#{id := Id, topic_filter := _MQTopicFilter} = MQ) ->
    case find(Id) of
        {ok, ConsumerRef} ->
            {ok, ConsumerRef};
        not_found ->
            case emqx_mq_sup:start_consumer(_ChildId = Id, [MQ]) of
                {ok, ConsumerRef} ->
                    {ok, ConsumerRef};
                {error, _} = _Error ->
                    ?tp_debug(mq_consumer_find_consumer_error, #{
                        error => _Error, mq_topic_filter => _MQTopicFilter
                    }),
                    not_found
            end
    end.

try_register_consumer(#{id := Id, topic_filter := MQTopicFilter} = _MQ) ->
    case emqx_mq_registry:find(MQTopicFilter) of
        {ok, #{id := Id}} ->
            case global:register_name(global_name(Id), self()) of
                yes ->
                    ok;
                no ->
                    {error, already_registered}
            end;
        _ ->
            {error, no_mq}
    end.

global_name(Id) ->
    {?MODULE, Id}.

send_to_consumer_server(Pid, InfoMsg) ->
    _ = erlang:send(Pid, #info_to_mq_server{message = InfoMsg}),
    ok.
