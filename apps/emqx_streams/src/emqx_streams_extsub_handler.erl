%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_extsub_handler).

-moduledoc """
Multi-topic ExtSub handler for the external subscription to the streams.

See [ExtSub description](../emqx_extsub/README.md) for more details.

This module utilizes a single DS client to handle all client's subscriptions to the streams.

The module is quite straightforward:
* it ties DS subscriptions to client's stream subscriptions;
* receives messages from DS and delivers them to the client;
* blocks and unblocks DS subscriptions depending on the client's desired message count.

The mechanism of blocking is the following:
* when a client does not want more messages, we set global status to `blocked`;
* while blocked, we stop acking deliveries from DS;
* when a client wants more messages, we set global status to `unblocked` and ack all the
subscriptions that we stopped acknowledging.

Important: `stream' here means EMQX Streams, a feature of EMQX MQTT Broker and the corresponding
data structure associated with it.
DS streams are explicity called `DS streams' here.
""".

-behaviour(emqx_extsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_streams_internal.hrl").

%% ExtSub Handler callbacks

-export([
    handle_subscribe/4,
    handle_unsubscribe/4,
    handle_terminate/1,
    handle_delivered/4,
    handle_info/3
]).

%% DS Client callbacks

-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5,
    on_subscription_down/4
]).

-record(stream_status_unblocked, {}).
-record(stream_status_blocked, {
    unblock_fns :: [fun((handler()) -> handler())]
}).

-type stream_status() :: #stream_status_unblocked{} | #stream_status_blocked{}.

-record(status_blocked, {}).
-record(status_unblocked, {}).

-type status() :: #status_blocked{} | #status_unblocked{}.

%% DS Client subscription ID
-type ds_sub_id() :: reference().

-record(subscribe_params, {
    name :: binary(),
    start_from :: binary(),
    topic_filter :: binary(),
    full_topic_filter :: binary()
}).

-type subscribe_params() :: #subscribe_params{}.

-record(shard_progress, {
    generation :: emqx_ds:generation(),
    last_timestamp_us :: emqx_ds:time()
}).

-type shard_progress() :: #shard_progress{}.

-record(stream_state, {
    stream :: emqx_streams_types:stream(),
    start_time_us :: emqx_ds:time(),
    progress :: #{emqx_ds:shard() => shard_progress()},
    ds_sub_id :: ds_sub_id(),
    status :: stream_status(),
    %% Stream may disappear, we need to store the subscribe params to be able to re-subscribe
    subscribe_params :: subscribe_params()
}).

-type stream_state() :: #stream_state{}.

-record(state, {
    send_fn :: function(),
    send_after_fn :: function(),
    %% Global status of the handler, indicating whether the extsub wants more messages or not
    status :: status(),
    %% Subscriptions to streams by their subscription ID in emqx_ds_client
    %% (Note that stream here means EMQX Streams, not a single DS stream).
    %% There may be multiple subscriptions to the same stream by different topics, like
    %% "$s/earliest/t/#" and "$sp/0/latest/t/#".
    ds_subs :: #{ds_sub_id() => stream_state()},
    by_topic_filter :: #{emqx_types:topic() => ds_sub_id()},
    %% Topics that correspond to unknown or deleted streams
    %% We always hope that the streams will be created eventually
    unknown_topic_filters :: #{emqx_types:topic() => subscribe_params()},
    check_stream_status_tref :: reference() | undefined
}).

-type state() :: #state{}.

-record(h, {
    state :: state(),
    ds_client :: emqx_ds_client:t()
}).

-type handler() :: #h{}.

%% Self-initiated messages

-record(check_stream_status, {}).

-define(START_FROM_USER_PROP, <<"stream-offset">>).
-define(START_FROM_USER_PROP_LEGACY, <<"$stream.start-from">>).
-define(START_FROM_DEFAULT_VALUE, <<"latest">>).

%%------------------------------------------------------------------------------------
%% ExtSub Handler callbacks
%%------------------------------------------------------------------------------------

handle_subscribe(
    _SubscribeType,
    SubscribeCtx,
    Handler0,
    TopicFilter
) ->
    Handler1 = init_handler(Handler0, SubscribeCtx),
    case check_stream_subscribe_topic_filter(SubscribeCtx, TopicFilter) of
        {ok, SubscribeParams} ->
            %% Topic filter is formally valid, try to subscribe
            case subscribe(Handler1, SubscribeParams) of
                {ok, Handler} ->
                    {ok, schedule_check_stream_status(Handler)};
                ?err_unrec(Reason) ->
                    ?tp(error, streams_extsub_handler_subscribe_error, #{
                        reason => Reason,
                        topic_filter => TopicFilter,
                        recoverable => false
                    }),
                    ignore;
                ?err_rec(Reason) ->
                    ?tp(info, streams_extsub_handler_subscribe_error, #{
                        reason => Reason,
                        topic_filter => TopicFilter,
                        recoverable => true
                    }),
                    %% Since the topic filter is formally valid, we add it to the unknown topic filters
                    %% to be checked later.
                    Handler = add_unknown_stream(Handler1, SubscribeParams),
                    {ok, schedule_check_stream_status(Handler)}
            end;
        ignore ->
            ignore;
        ?err_unrec(Reason) ->
            ?tp(warning, streams_extsub_handler_subscribe_error, #{
                reason => Reason,
                topic_filter => TopicFilter,
                recoverable => false
            }),
            ignore
    end.

handle_unsubscribe(
    _UnsubscribeType,
    _UnsubscribeCtx,
    #h{
        state =
            #state{by_topic_filter = ByTopicFilter, unknown_topic_filters = UnknownTopicFilters} =
                State0
    } = Handler,
    FullTopicFilter
) ->
    case ByTopicFilter of
        #{FullTopicFilter := DSSubId} ->
            unsubscribe(Handler, FullTopicFilter, DSSubId);
        _ ->
            case UnknownTopicFilters of
                #{FullTopicFilter := _} ->
                    State = State0#state{
                        unknown_topic_filters = maps:remove(FullTopicFilter, UnknownTopicFilters)
                    },
                    Handler#h{state = State};
                _ ->
                    Handler
            end
    end.

handle_terminate(#h{state = State, ds_client = DSClient}) ->
    ?tp(debug, handle_terminate, #{state => State}),
    _ = emqx_ds_client:destroy(DSClient, State),
    ok.

handle_delivered(
    Handler,
    #{desired_message_count := DesiredCount} = _DeliveredCtx,
    _Message,
    _Ack
) ->
    update_blocking_status(Handler, DesiredCount).

handle_info(Handler, #{desired_message_count := DesiredCount} = _InfoCtx, Info) ->
    handle_info(update_blocking_status(Handler, DesiredCount), Info).

%%------------------------------------------------------------------------------------
%% DS Client callbacks
%%------------------------------------------------------------------------------------

get_current_generation(DSSubId, Shard, #state{ds_subs = DSSubs}) ->
    case DSSubs of
        #{
            DSSubId := #stream_state{
                progress = #{Shard := #shard_progress{generation = Generation}}
            }
        } ->
            Generation;
        #{DSSubId := #stream_state{}} ->
            %% Should not happen
            0;
        _ ->
            error({unknown_subscription, #{ds_sub_id => DSSubId}})
    end.

on_advance_generation(
    DSSubId, Shard, Generation, #state{ds_subs = DSSubs} = State
) ->
    case DSSubs of
        #{DSSubId := StreamState0} ->
            StreamState = advance_shard_generation(StreamState0, Shard, Generation),
            update_stream_state(State, DSSubId, StreamState);
        _ ->
            error({unknown_subscription, #{ds_sub_id => DSSubId}})
    end.

get_iterator(DSSubId, {Shard, _} = _Slab, _DSStream, #state{ds_subs = DSSubs}) ->
    case DSSubs of
        #{DSSubId := #stream_state{} = StreamState} ->
            StartTimeUs = get_shard_start_time_us(StreamState, Shard),
            {undefined, #{start_time => StartTimeUs}};
        _ ->
            %% Should never happen
            error({unknown_subscription, #{ds_sub_id => DSSubId}})
    end.

on_new_iterator(
    DSSubId, _Slab, _DSStream, _It, #state{ds_subs = DSSubs} = State
) ->
    case DSSubs of
        #{DSSubId := #stream_state{}} ->
            {subscribe, State};
        _ ->
            %% Should never happen
            error({unknown_subscription, #{ds_sub_id => DSSubId}})
    end.

on_unrecoverable_error(DSSubId, Slab, _DSStream, Error, #state{ds_subs = DSSubs} = State) ->
    ?tp(error, streams_extsub_handler_on_unrecoverable_error, #{slab => Slab, error => Error}),
    case DSSubs of
        #{DSSubId := #stream_state{status = #stream_status_blocked{}} = StreamState} ->
            update_stream_state(State, DSSubId, StreamState#stream_state{
                status = #stream_status_unblocked{}
            });
        _ ->
            State
    end.

on_subscription_down(DSSubId, Slab, _DSStream, #state{ds_subs = DSSubs} = State) ->
    ?tp(error, streams_extsub_handler_on_subscription_down, #{slab => Slab}),
    case DSSubs of
        #{DSSubId := #stream_state{status = #stream_status_blocked{}} = StreamState} ->
            update_stream_state(State, DSSubId, StreamState#stream_state{
                status = #stream_status_unblocked{}
            });
        _ ->
            State
    end.

%%------------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------------

subscribe(
    Handler,
    #subscribe_params{
        name = Name,
        topic_filter = MaybeTopicFilter,
        start_from = StartFromBin,
        full_topic_filter = FullTopicFilter
    } =
        SubscribeParams
) ->
    maybe
        ok ?= validate_new_topic_filter(Handler, SubscribeParams),
        {ok, StartFrom} ?= parse_offset(StartFromBin),
        {ok, Stream} ?= find_stream(Name, MaybeTopicFilter),
        #h{
            state = #state{by_topic_filter = ByTopicFilter, ds_subs = DSSubs} = State0,
            ds_client = DSClient0
        } = Handler,
        DSSubId = make_ref(),
        StartTimeUs = start_time_us(Stream, StartFrom),
        ?tp_debug(streams_extsub_handler_subscribe, #{
            stream => Name, start_from => StartFrom, start_time_us => StartTimeUs
        }),
        {ok, Progress} ?= init_progress(Stream, StartTimeUs),
        StreamState = #stream_state{
            stream = Stream,
            start_time_us = StartTimeUs,
            progress = Progress,
            ds_sub_id = DSSubId,
            status = #stream_status_unblocked{},
            subscribe_params = SubscribeParams
        },
        State1 = State0#state{
            by_topic_filter = ByTopicFilter#{FullTopicFilter => DSSubId},
            ds_subs = DSSubs#{DSSubId => StreamState}
        },
        {ok, DSClient, State} = emqx_streams_message_db:subscribe(
            Stream, DSClient0, DSSubId, State1
        ),
        {ok, Handler#h{state = State, ds_client = DSClient}}
    end.

unsubscribe(
    #h{
        state = State0,
        ds_client = DSClient0
    } =
        Handler,
    FullTopicFilter,
    DSSubId
) ->
    {ok, DSClient, #state{by_topic_filter = ByTopicFilter, ds_subs = DSSubs} = State1} = emqx_ds_client:unsubscribe(
        DSClient0, DSSubId, State0
    ),
    State = State1#state{
        by_topic_filter = maps:remove(FullTopicFilter, ByTopicFilter),
        ds_subs = maps:remove(DSSubId, DSSubs)
    },
    Handler#h{state = State, ds_client = DSClient}.

handle_info(
    #h{state = #state{ds_subs = DSSubs} = State0, ds_client = DSClient0} = Handler, {generic, Info}
) ->
    case emqx_ds_client:dispatch_message(Info, DSClient0, State0) of
        ignore ->
            {ok, Handler};
        {DSClient, State} ->
            {ok, Handler#h{ds_client = DSClient, state = State}};
        {data, DSSubId, DSStream, SubHandle, DSReply} ->
            case DSSubs of
                #{DSSubId := StreamState} ->
                    handle_ds_info(Handler, StreamState, DSSubId, DSStream, SubHandle, DSReply);
                _ ->
                    {ok, Handler}
            end
    end;
handle_info(Handler0, #check_stream_status{}) ->
    Handler1 = check_active_streams_status(Handler0),
    Handler = check_unknown_stream_status(Handler1),
    {ok, reschedule_check_stream_status(Handler)}.

handle_ds_info(
    #h{state = #state{status = Status}} = Handler,
    StreamState0,
    DSSubId,
    DSStream,
    SubHandle,
    DSReply
) ->
    ok = inc_received_message_stat(DSReply),
    case DSReply of
        #ds_sub_reply{payload = {ok, end_of_stream}, ref = SubRef} ->
            case Status of
                #status_blocked{} ->
                    StreamState = block_stream(
                        StreamState0,
                        fun(Hndlr) ->
                            complete_subscribed_dsstream(Hndlr, DSSubId, SubRef, DSStream)
                        end
                    ),
                    {ok, update_stream_state(Handler, DSSubId, StreamState)};
                #status_unblocked{} ->
                    {ok, complete_subscribed_dsstream(Handler, DSSubId, SubRef, DSStream)}
            end;
        #ds_sub_reply{payload = {ok, _It, TTVs}, seqno = SeqNo, size = _Size} ->
            case Status of
                #status_blocked{} ->
                    Shard = get_shard_by_dsstream(StreamState0, DSStream),
                    LastTimestampUs0 = get_shard_start_time_us(StreamState0, Shard),
                    {LastTimestampUs, Messages} = to_messages(Shard, LastTimestampUs0, TTVs),
                    StreamState1 = advance_shard_last_time(StreamState0, Shard, LastTimestampUs),
                    StreamState = block_stream(
                        StreamState1,
                        fun(Hndlr) ->
                            ok = suback(Hndlr, DSSubId, SubHandle, SeqNo),
                            Hndlr
                        end
                    ),
                    {ok, update_stream_state(Handler, DSSubId, StreamState), Messages};
                #status_unblocked{} ->
                    ok = suback(Handler, DSSubId, SubHandle, SeqNo),
                    Shard = get_shard_by_dsstream(StreamState0, DSStream),
                    LastTimestampUs0 = get_shard_start_time_us(StreamState0, Shard),
                    {LastTimestampUs, Messages} = to_messages(Shard, LastTimestampUs0, TTVs),
                    StreamState1 = advance_shard_last_time(StreamState0, Shard, LastTimestampUs),
                    StreamState = StreamState1#stream_state{status = #stream_status_unblocked{}},
                    {ok, update_stream_state(Handler, DSSubId, StreamState), Messages}
            end
    end.

%% Blocking/unblocking streams

update_blocking_status(#h{state = #state{status = Status} = State0} = Handler0, DesiredCount) ->
    case {Status, DesiredCount} of
        {#status_unblocked{}, 0} ->
            Handler0#h{state = State0#state{status = #status_blocked{}}};
        {#status_unblocked{}, N} when N > 0 ->
            Handler0;
        {#status_blocked{}, 0} ->
            Handler0;
        {#status_blocked{}, N} when N > 0 ->
            Handler = #h{state = State} = unblock_streams(Handler0),
            Handler#h{state = State#state{status = #status_unblocked{}}}
    end.

unblock_streams(#h{state = #state{ds_subs = DSSubs}} = Handler) ->
    maps:fold(
        fun
            (_SubId, #stream_state{status = #stream_status_unblocked{}}, HandlerAcc) ->
                HandlerAcc;
            (
                DSSubId,
                #stream_state{status = #stream_status_blocked{unblock_fns = UnblockFns}} =
                    StreamState0,
                HandlerAcc0
            ) ->
                HandlerAcc = unblock_stream(HandlerAcc0, UnblockFns),
                StreamState = StreamState0#stream_state{status = #stream_status_unblocked{}},
                update_stream_state(HandlerAcc, DSSubId, StreamState)
        end,
        Handler,
        DSSubs
    ).

unblock_stream(Handler, UnblockFns) ->
    lists:foldl(
        fun(UnblockFn, HandlerAcc) ->
            UnblockFn(HandlerAcc)
        end,
        Handler,
        UnblockFns
    ).

%% Managament of DS streams in Stream states.

complete_subscribed_dsstream(
    #h{state = State0, ds_client = DSClient0} = Handler, _DSSubId, SubRef, _DSStream
) ->
    {DSClient, State} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    Handler#h{ds_client = DSClient, state = State}.

get_shard_by_dsstream(#stream_state{stream = Stream}, DSStream) ->
    {ok, {Shard, _Generation}} = emqx_streams_message_db:slab_of_dsstream(Stream, DSStream),
    Shard.

advance_shard_generation(
    #stream_state{start_time_us = StartTimeUs, progress = Progress0} =
        StreamState0,
    Shard,
    Generation
) ->
    ShardProgress =
        case Progress0 of
            #{Shard := #shard_progress{} = ShardProgress0} ->
                ShardProgress0#shard_progress{generation = Generation};
            _ ->
                #shard_progress{generation = Generation, last_timestamp_us = StartTimeUs}
        end,
    Progress = Progress0#{Shard => ShardProgress},
    StreamState0#stream_state{progress = Progress}.

advance_shard_last_time(
    #stream_state{progress = Progress0} = StreamState0,
    Shard,
    LastSeenTimestampUs
) ->
    ShardProgress =
        case Progress0 of
            #{Shard := #shard_progress{} = ShardProgress0} ->
                ShardProgress0#shard_progress{last_timestamp_us = LastSeenTimestampUs + 1};
            _ ->
                %% Should not happen
                #shard_progress{
                    last_timestamp_us = LastSeenTimestampUs + 1, generation = 0
                }
        end,
    Progress = Progress0#{Shard => ShardProgress},
    StreamState0#stream_state{progress = Progress}.

get_shard_start_time_us(#stream_state{progress = Progress, start_time_us = StartTimeUs}, Shard) ->
    case Progress of
        #{Shard := #shard_progress{last_timestamp_us = LastTimestampUs}} ->
            LastTimestampUs;
        _ ->
            StartTimeUs
    end.

block_stream(#stream_state{status = #stream_status_unblocked{}} = StreamState, UnblockFn) ->
    StreamState#stream_state{status = #stream_status_blocked{unblock_fns = [UnblockFn]}};
block_stream(
    #stream_state{status = #stream_status_blocked{unblock_fns = UnblockFns}} = StreamState,
    UnblockFn
) ->
    StreamState#stream_state{
        status = #stream_status_blocked{unblock_fns = [UnblockFn | UnblockFns]}
    }.

%% Subscribe parsers and validators

start_time_us(Stream, earliest) ->
    DataRetentionPeriodUs = erlang:convert_time_unit(
        emqx_streams_prop:data_retention_period(Stream), millisecond, microsecond
    ),
    erlang:system_time(microsecond) - DataRetentionPeriodUs;
start_time_us(_Stream, latest) ->
    erlang:system_time(microsecond);
start_time_us(Stream, TimestampUs) when is_integer(TimestampUs) ->
    DataRetentionPeriodUs = erlang:convert_time_unit(
        emqx_streams_prop:data_retention_period(Stream), millisecond, microsecond
    ),
    max(TimestampUs, erlang:system_time(microsecond) - DataRetentionPeriodUs).

parse_offset(<<"earliest">>) ->
    {ok, earliest};
parse_offset(<<"latest">>) ->
    {ok, latest};
parse_offset(OffsetBin) ->
    maybe
        {Timestamp, <<>>} ?= string:to_integer(OffsetBin),
        {ok, Timestamp}
    else
        _ ->
            ?err_unrec(invalid_offset)
    end.

validate_new_topic_filter(
    #h{
        state = #state{by_topic_filter = ByTopicFilter, unknown_topic_filters = UnknownTopicFilters}
    },
    #subscribe_params{full_topic_filter = FullTopicFilter}
) ->
    case
        maps:is_key(FullTopicFilter, ByTopicFilter) or
            maps:is_key(FullTopicFilter, UnknownTopicFilters)
    of
        true -> ?err_unrec({topic_filter_already_present, #{topic_filter => FullTopicFilter}});
        false -> ok
    end.

split_topic_filter(TopicFilter) ->
    case binary:split(TopicFilter, <<"/">>) of
        [First, Rest] -> {ok, First, Rest};
        _ -> ?err_unrec(invalid_topic_filter)
    end.

split_name_topic(NameTopic) ->
    case binary:split(NameTopic, <<"/">>) of
        [Name, Topic] -> {Name, Topic};
        _ -> {NameTopic, undefined}
    end.

find_stream(Name, TopicFilter) ->
    case emqx_streams_registry:find(Name) of
        {ok, Stream} when TopicFilter =:= undefined ->
            {ok, Stream};
        {ok, #{topic_filter := TopicFilter} = Stream} ->
            {ok, Stream};
        {ok, #{topic_filter := ExistingTopicFilter}} ->
            ?err_rec(
                {stream_not_found, #{
                    name => Name,
                    topic_filter => TopicFilter,
                    existing_topic_filter => ExistingTopicFilter
                }}
            );
        not_found ->
            case emqx_streams_config:auto_create(Name, TopicFilter) of
                {true, DefaultStream} ->
                    case emqx_streams_registry:create(DefaultStream) of
                        {ok, Stream} ->
                            {ok, Stream};
                        {error, Reason} ->
                            ?err_rec(
                                {cannot_auto_create_stream, #{
                                    stream => DefaultStream, reason => Reason
                                }}
                            )
                    end;
                false ->
                    ?err_rec({stream_not_found, #{name => Name, topic_filter => TopicFilter}})
            end
    end.

%% Other helpers

to_messages(Shard, LastTimestampUs, TTVs) ->
    {NewLastTimestampUs, Messages} = lists:foldl(
        fun({Topic, Time, Payload}, {LastTimestampUsAcc, MessagesAcc}) ->
            case Time >= LastTimestampUsAcc of
                true -> {Time, [decode_message(Shard, Topic, Time, Payload) | MessagesAcc]};
                false -> {LastTimestampUsAcc, MessagesAcc}
            end
        end,
        {LastTimestampUs, []},
        TTVs
    ),
    {NewLastTimestampUs, lists:reverse(Messages)}.

suback(#h{state = #state{ds_subs = DSSubs}}, DSSubId, SubHandle, SeqNo) ->
    case DSSubs of
        #{DSSubId := #stream_state{stream = Stream}} ->
            ok = emqx_streams_message_db:suback(Stream, SubHandle, SeqNo);
        _ ->
            ok
    end.

init_handler(undefined, #{send_after := SendAfterFn, send := SendFn} = _Ctx) ->
    State = #state{
        send_fn = SendFn,
        send_after_fn = SendAfterFn,
        status = #status_unblocked{},
        ds_subs = #{},
        by_topic_filter = #{},
        unknown_topic_filters = #{},
        check_stream_status_tref = undefined
    },
    DSClient = emqx_streams_message_db:create_client(?MODULE),
    #h{state = State, ds_client = DSClient};
init_handler(#h{} = Handler, _Ctx) ->
    Handler.

update_stream_state(#h{state = State} = Handler, DSSubId, StreamState) ->
    Handler#h{state = update_stream_state(State, DSSubId, StreamState)};
update_stream_state(#state{ds_subs = DSSubs} = State, DSSubId, StreamState) ->
    State#state{ds_subs = DSSubs#{DSSubId => StreamState}}.

decode_message(_Shard, ?STREAMS_MESSAGE_DB_TOPIC(_TF, _StreamId, Key), Time, Payload) ->
    Message = emqx_streams_message_db:decode_message(Payload),
    add_properties(Message, #{
        <<"ts">> => integer_to_binary(Time),
        <<"key">> => Key
    }).

add_properties(Message, AddProperties) when is_map(AddProperties) ->
    Props = emqx_message:get_header(properties, Message, #{}),
    UserProperties0 = maps:get('User-Property', Props, []),
    UserPropMap = maps:merge(maps:from_list(UserProperties0), AddProperties),
    UserProperties = maps:to_list(UserPropMap),
    emqx_message:set_header(properties, Props#{'User-Property' => UserProperties}, Message).

init_progress(Stream, StartTimeUs) ->
    case emqx_streams_message_db:find_generations(Stream, StartTimeUs) of
        {ok, Generations} ->
            {ok,
                maps:map(
                    fun(_Shard, Generation) ->
                        #shard_progress{generation = Generation, last_timestamp_us = StartTimeUs}
                    end,
                    Generations
                )};
        {error, Reason} ->
            ?err_rec({cannot_init_generations, #{stream => Stream, reason => Reason}})
    end.

inc_received_message_stat(#ds_sub_reply{payload = {ok, _It, _TTVs}, size = Size}) ->
    emqx_streams_metrics:inc(ds, received_messages, Size);
inc_received_message_stat(#ds_sub_reply{}) ->
    ok.

%% Management of stream appearing/disappearing.

add_unknown_stream(
    #h{state = #state{unknown_topic_filters = UnknownTopicFilters0} = State0} = Handler,
    #subscribe_params{full_topic_filter = FullTopicFilter} = SubscribeParams
) ->
    UnknownTopicFilters = UnknownTopicFilters0#{FullTopicFilter => SubscribeParams},
    State = State0#state{unknown_topic_filters = UnknownTopicFilters},
    Handler#h{state = State}.

schedule_check_stream_status(#h{state = State} = Handler) ->
    Handler#h{state = schedule_check_stream_status(State)};
schedule_check_stream_status(
    #state{unknown_topic_filters = UnknownTopicFilters, by_topic_filter = ByTopicFilter} = State
) ->
    schedule_check_stream_status(State, maps:size(UnknownTopicFilters) + maps:size(ByTopicFilter)).

schedule_check_stream_status(#state{check_stream_status_tref = TRef} = State, 0) ->
    _ = emqx_utils:cancel_timer(TRef),
    State#state{check_stream_status_tref = undefined};
schedule_check_stream_status(
    #state{check_stream_status_tref = undefined, send_after_fn = SendAfterFn} = State, N
) when N > 0 ->
    Interval = emqx_streams_config:check_stream_status_interval(),
    TRef = SendAfterFn(Interval, #check_stream_status{}),
    State#state{check_stream_status_tref = TRef};
schedule_check_stream_status(State, _N) ->
    State.

reschedule_check_stream_status(
    #h{state = #state{check_stream_status_tref = TRef} = State} = Handler
) ->
    _ = emqx_utils:cancel_timer(TRef),
    schedule_check_stream_status(Handler#h{
        state = State#state{check_stream_status_tref = undefined}
    }).

check_unknown_stream_status(
    #h{state = #state{unknown_topic_filters = UnknownTopicFilters0} = State0} = Handler0
) ->
    %% Set unknown topic filters to empty to avoid `topic_filter_already_present' errors
    %% when trying to subscribe
    Handler1 = Handler0#h{state = State0#state{unknown_topic_filters = #{}}},
    {Handler, UnknownTopicFilters} = maps:fold(
        fun(FullTopicFilter, SubscribeParams, {HandlerAcc0, UnknownTopicFiltersAcc}) ->
            case subscribe(HandlerAcc0, SubscribeParams) of
                {ok, HandlerAcc} ->
                    {HandlerAcc, UnknownTopicFiltersAcc};
                ?err_unrec(Reason) ->
                    ?tp(error, streams_extsub_handler_delayed_subscribe_error, #{
                        reason => Reason,
                        topic_filter => FullTopicFilter,
                        recoverable => false
                    }),
                    %% Should never happen, however, do not retry on unrecoverable error
                    {HandlerAcc0, UnknownTopicFiltersAcc};
                ?err_rec(Reason) ->
                    ?tp(info, streams_extsub_handler_delayed_subscribe_error, #{
                        reason => Reason,
                        topic_filter => FullTopicFilter,
                        recoverable => true
                    }),
                    {HandlerAcc0, UnknownTopicFiltersAcc#{FullTopicFilter => SubscribeParams}}
            end
        end,
        {Handler1, #{}},
        UnknownTopicFilters0
    ),
    #h{state = State} = Handler,
    Handler#h{state = State#state{unknown_topic_filters = UnknownTopicFilters}}.

check_active_streams_status(
    #h{
        state = #state{
            by_topic_filter = ByTopicFilter, unknown_topic_filters = UnknownTopicFilters0
        }
    } = Handler0
) ->
    {Handler, UnknownTopicFilters} = maps:fold(
        fun(FullTopicFilter, DSSubId, {HandlerAcc0, UnknownTopicFiltersAcc}) ->
            #h{state = #state{ds_subs = DSSubs}} = HandlerAcc0,
            #stream_state{
                stream = #{id := Id} = Stream,
                subscribe_params = SubscribeParams
            } = maps:get(DSSubId, DSSubs),
            case emqx_streams_registry:find(emqx_streams_prop:name(Stream)) of
                {ok, #{id := Id}} ->
                    {HandlerAcc0, UnknownTopicFiltersAcc};
                _ ->
                    HandlerAcc = unsubscribe(HandlerAcc0, FullTopicFilter, DSSubId),
                    {HandlerAcc, UnknownTopicFiltersAcc#{FullTopicFilter => SubscribeParams}}
            end
        end,
        {Handler0, UnknownTopicFilters0},
        ByTopicFilter
    ),
    #h{state = State} = Handler,
    Handler#h{state = State#state{unknown_topic_filters = UnknownTopicFilters}}.

check_stream_subscribe_topic_filter(_Ctx, <<"$s/", TopicFilter0/binary>> = FullTopicFilter) ->
    maybe
        {ok, StartFrom, TopicFilter} ?= split_topic_filter(TopicFilter0),
        Name = emqx_streams_prop:default_name_from_topic(TopicFilter),
        {ok, #subscribe_params{
            name = Name,
            start_from = StartFrom,
            topic_filter = TopicFilter,
            full_topic_filter = FullTopicFilter
        }}
    end;
check_stream_subscribe_topic_filter(Ctx, <<"$stream/", NameTopicFilter/binary>> = FullTopicFilter) ->
    SubOpts = maps:get(subopts, Ctx, #{}),
    SubProps = maps:get(sub_props, SubOpts, #{}),
    UserProperties = maps:get('User-Property', SubProps, []),
    StartFrom =
        case proplists:lookup(?START_FROM_USER_PROP, UserProperties) of
            none ->
                proplists:get_value(
                    ?START_FROM_USER_PROP_LEGACY,
                    UserProperties,
                    ?START_FROM_DEFAULT_VALUE
                );
            {?START_FROM_USER_PROP, Value} ->
                Value
        end,
    maybe
        {Name, TopicFilter} = split_name_topic(NameTopicFilter),
        ok ?= validate_name(Name),
        {ok, #subscribe_params{
            name = Name,
            start_from = StartFrom,
            topic_filter = TopicFilter,
            full_topic_filter = FullTopicFilter
        }}
    end;
check_stream_subscribe_topic_filter(_Ctx, _TopicFilter) ->
    ignore.

validate_name(Name) ->
    case emqx_streams_schema:validate_name(Name) of
        ok ->
            ok;
        {error, Reason} ->
            ?err_unrec({invalid_name, #{name => Name, reason => Reason}})
    end.
