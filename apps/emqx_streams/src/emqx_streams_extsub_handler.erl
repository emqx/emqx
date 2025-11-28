%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_extsub_handler).

-moduledoc """
Handler for the external subscription to the streams.

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
    handle_unsubscribe/3,
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
-type sub_id() :: reference().

-define(all_shards, all).
-define(is_subscribed_to_shard(Shard, SubShard),
    (SubShard =:= Shard orelse SubShard =:= ?all_shards)
).

-record(shard_progress, {
    generation :: emqx_ds:generation(),
    last_timestamp_us :: emqx_ds:time()
}).

-type shard_progress() :: #shard_progress{}.

-record(stream_state, {
    stream :: emqx_streams_types:stream(),
    shard :: emqx_ds:shard() | ?all_shards,
    start_time_us :: emqx_ds:time(),
    progress :: #{emqx_ds:shard() => shard_progress()},
    shard_by_dsstream :: #{emqx_ds:stream() => emqx_ds:shard()},
    sub_id :: sub_id(),
    status :: stream_status()
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
    %% "$s/all/earliest/t/#" and "$s/0/latest/t/#".
    subs :: #{sub_id() => stream_state()},
    by_topic_filter :: #{emqx_types:topic() => sub_id()},
    %% Topics that correspond to unknown or deleted streams
    %% We always hope that the streams will be created eventually
    unknown_topic_filters :: sets:set(emqx_streams_types:stream_topic()),
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
-record(complete_stream, {
    sub_id :: sub_id(),
    slab :: emqx_ds:slab(),
    ds_stream :: emqx_ds:stream()
}).

%%------------------------------------------------------------------------------------
%% ExtSub Handler callbacks
%%------------------------------------------------------------------------------------

handle_subscribe(
    _SubscribeType,
    SubscribeCtx,
    Handler0,
    <<"$s/", SubscribeTopicFilter/binary>> = _FullTopicFilter
) ->
    Handler1 = init_handler(Handler0, SubscribeCtx),
    case subscribe(Handler1, SubscribeTopicFilter) of
        {ok, Handler} ->
            {ok, schedule_check_stream_status(Handler)};
        ?err_unrec(Reason) ->
            ?tp(error, streams_extsub_handler_subscribe_error, #{
                reason => Reason,
                subscribe_topic_filter => SubscribeTopicFilter,
                recoverable => false
            }),
            ignore;
        ?err_rec(Reason) ->
            ?tp(info, streams_extsub_handler_subscribe_error, #{
                reason => Reason,
                subscribe_topic_filter => SubscribeTopicFilter,
                recoverable => true
            }),
            Handler = add_unknown_stream(Handler1, SubscribeTopicFilter),
            {ok, schedule_check_stream_status(Handler)}
    end;
handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, _FullTopicFilter) ->
    ignore.

handle_unsubscribe(
    _UnsubscribeType,
    #h{
        state =
            #state{by_topic_filter = ByTopicFilter0, unknown_topic_filters = UnknownTopicFilters} =
                State0
    } = Handler,
    <<"$s/", SubscribeTopicFilter/binary>>
) ->
    case ByTopicFilter0 of
        #{SubscribeTopicFilter := SubId} ->
            unsubscribe(Handler, SubscribeTopicFilter, SubId);
        _ ->
            case sets:is_element(SubscribeTopicFilter, UnknownTopicFilters) of
                true ->
                    State = State0#state{
                        unknown_topic_filters = sets:del_element(
                            SubscribeTopicFilter, UnknownTopicFilters
                        )
                    },
                    Handler#h{state = State};
                false ->
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

get_current_generation(SubId, Shard, #state{subs = Subs}) ->
    ?tp_debug(streams_extsub_handler_get_current_generation, #{sub_id => SubId, shard => Shard}),
    case Subs of
        #{SubId := #stream_state{progress = #{Shard := #shard_progress{generation = Generation}}}} ->
            Generation;
        #{SubId := #stream_state{}} ->
            %% Should not happen
            0;
        _ ->
            error({unknown_subscription, #{sub_id => SubId}})
    end.

on_advance_generation(
    SubId, Shard, Generation, #state{subs = Subs} = State
) ->
    ?tp_debug(streams_extsub_handler_on_advance_generation, #{
        sub_id => SubId, shard => Shard, generation => Generation
    }),
    case Subs of
        #{SubId := StreamState0} ->
            StreamState = advance_shard_generation(StreamState0, Shard, Generation),
            update_stream_state(State, SubId, StreamState);
        _ ->
            error({unknown_subscription, #{sub_id => SubId}})
    end.

get_iterator(SubId, {Shard, _} = Slab, DSStream, #state{subs = Subs, send_fn = SendFn}) ->
    case Subs of
        #{SubId := #stream_state{shard = SubShard} = StreamState} when
            ?is_subscribed_to_shard(Shard, SubShard)
        ->
            StartTimeUs = get_shard_start_time_us(StreamState, Shard),
            StartTimeMs = erlang:convert_time_unit(StartTimeUs, microsecond, millisecond),
            ?tp_debug(streams_extsub_handler_get_iterator, #{
                status => create,
                sub_id => SubId,
                slab => Slab,
                ds_stream => DSStream,
                start_time => StartTimeMs
            }),
            {undefined, #{start_time => StartTimeMs}};
        #{SubId := _} ->
            %% Other shards we are not interested in
            SendFn(#complete_stream{
                sub_id = SubId, slab = Slab, ds_stream = DSStream
            }),
            ?tp_debug(streams_extsub_handler_get_iterator, #{
                status => create_end_of_stream,
                sub_id => SubId,
                slab => Slab,
                ds_stream => DSStream
            }),
            {ok, end_of_stream};
        _ ->
            %% Should never happen
            error({unknown_subscription, #{sub_id => SubId}})
    end.

on_new_iterator(
    SubId, {Shard, _} = Slab, DSStream, _It, #state{subs = Subs, send_fn = SendFn} = State
) ->
    case Subs of
        #{SubId := #stream_state{shard = SubShard}} when
            SubShard =:= Shard orelse SubShard =:= ?all_shards
        ->
            ?tp_debug(streams_extsub_handler_on_new_iterator_subscribe, #{
                status => subscribe,
                sub_id => SubId,
                slab => Slab,
                ds_stream => DSStream
            }),
            {subscribe, add_dsstream_to_index(State, SubId, DSStream, Shard)};
        #{SubId := _} ->
            SendFn(#complete_stream{
                sub_id = SubId, slab = Slab, ds_stream = DSStream
            }),
            {ignore, State};
        _ ->
            %% Should never happen
            error({unknown_subscription, #{sub_id => SubId}})
    end.

on_unrecoverable_error(SubId, Slab, _DSStream, Error, State) ->
    ?tp(error, streams_extsub_handler_on_unrecoverable_error, #{slab => Slab, error => Error}),
    case State of
        #{SubId := #stream_state{status = #stream_status_blocked{}} = StreamState} ->
            update_stream_state(State, SubId, StreamState#stream_state{
                status = #stream_status_unblocked{}
            });
        _ ->
            State
    end.

on_subscription_down(SubId, Slab, _DSStream, State) ->
    ?tp(error, streams_extsub_handler_on_subscription_down, #{slab => Slab}),
    case State of
        #{SubId := #stream_state{status = #stream_status_blocked{}} = StreamState} ->
            update_stream_state(State, SubId, StreamState#stream_state{
                status = #stream_status_unblocked{}
            });
        _ ->
            State
    end.

%%------------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------------

subscribe(Handler, SubscribeTopicFilter) ->
    maybe
        {ok, Partition, Rest1} ?= split_topic_filter(SubscribeTopicFilter),
        {ok, OffsetBin, TopicFilter} ?= split_topic_filter(Rest1),
        {ok, Offset} ?= parse_offset(OffsetBin),
        ok ?= validate_new_topic_filter(Handler, SubscribeTopicFilter),
        {ok, Stream} ?= find_stream(TopicFilter),
        {ok, SubShard} ?= validate_partition(Stream, Partition),
        #h{
            state = #state{by_topic_filter = ByTopicFilter, subs = Subs} = State0,
            ds_client = DSClient0
        } = Handler,
        SubId = make_ref(),
        StartTimeUs = start_time_us(Offset),
        {ok, Progress} ?= init_progress(Stream, SubShard, StartTimeUs),
        StreamState = #stream_state{
            stream = Stream,
            shard = SubShard,
            start_time_us = StartTimeUs,
            progress = Progress,
            shard_by_dsstream = #{},
            sub_id = SubId,
            status = #stream_status_unblocked{}
        },
        State1 = State0#state{
            by_topic_filter = ByTopicFilter#{SubscribeTopicFilter => SubId},
            subs = Subs#{SubId => StreamState}
        },
        ?tp_debug(streams_extsub_handler_handle_subscribe, #{
            sub_id => SubId,
            stream => Stream,
            shard => SubShard,
            progress => Progress,
            start_time_us => StartTimeUs
        }),
        {ok, DSClient, State} = emqx_streams_message_db:subscribe(Stream, DSClient0, SubId, State1),
        {ok, Handler#h{state = State, ds_client = DSClient}}
    end.

unsubscribe(
    #h{state = #state{by_topic_filter = ByTopicFilter, subs = Subs} = State0, ds_client = DSClient0} =
        Handler,
    SubscribeTopicFilter,
    SubId
) ->
    {ok, DSClient, #state{by_topic_filter = ByTopicFilter, subs = Subs} = State1} = emqx_ds_client:unsubscribe(
        DSClient0, SubId, State0
    ),
    State = State1#state{
        by_topic_filter = maps:remove(SubscribeTopicFilter, ByTopicFilter),
        subs = maps:remove(SubId, Subs)
    },
    Handler#h{state = State, ds_client = DSClient}.

handle_info(
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = Handler, {generic, Info}
) ->
    case emqx_ds_client:dispatch_message(Info, DSClient0, State0) of
        ignore ->
            {ok, Handler};
        {DSClient, State} ->
            {ok, Handler#h{ds_client = DSClient, state = State}};
        {data, SubId, DSStream, SubHandle, DSReply} ->
            case Subs of
                #{SubId := StreamState} ->
                    handle_ds_info(Handler, StreamState, SubId, DSStream, SubHandle, DSReply);
                _ ->
                    {ok, Handler}
            end
    end;
handle_info(
    #h{state = #state{subs = Subs}} = Handler, #complete_stream{
        sub_id = SubId, slab = Slab, ds_stream = DSStream
    }
) ->
    case Subs of
        #{SubId := _} ->
            {ok, complete_skipped_dsstream(Handler, SubId, Slab, DSStream)};
        _ ->
            {ok, Handler}
    end;
handle_info(Handler0, #check_stream_status{}) ->
    Handler1 = check_active_streams_status(Handler0),
    Handler = check_unknown_stream_status(Handler1),
    {ok, reschedule_check_stream_status(Handler)}.

handle_ds_info(
    #h{state = #state{status = Status}} = Handler,
    StreamState0,
    SubId,
    DSStream,
    SubHandle,
    DSReply
) ->
    case DSReply of
        #ds_sub_reply{payload = {ok, end_of_stream}, ref = SubRef} ->
            case Status of
                #status_blocked{} ->
                    StreamState = block_stream(
                        StreamState0,
                        fun(Hndlr) ->
                            complete_subscribed_dsstream(Hndlr, SubId, SubRef, DSStream)
                        end
                    ),
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => blocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => end_of_stream
                    }),
                    {ok, update_stream_state(Handler, SubId, StreamState)};
                #status_unblocked{} ->
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => unblocked,
                        sub_id => SubId,
                        stream_state => StreamState0,
                        payload => end_of_stream
                    }),
                    {ok, complete_subscribed_dsstream(Handler, SubId, SubRef, DSStream)}
            end;
        #ds_sub_reply{payload = {ok, _It, TTVs}, seqno = SeqNo, size = Size} ->
            case Status of
                #status_blocked{} ->
                    Shard = get_shard_by_dsstream(StreamState0, DSStream),
                    LastTimestampUs0 = get_shard_start_time_us(StreamState0, Shard),
                    {LastTimestampUs, Messages} = to_messages(Shard, LastTimestampUs0, TTVs),
                    StreamState1 = advance_shard_last_time(StreamState0, Shard, LastTimestampUs),
                    StreamState = block_stream(
                        StreamState1,
                        fun(Hndlr) ->
                            ok = suback(Hndlr, SubId, SubHandle, SeqNo),
                            Hndlr
                        end
                    ),
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => blocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => #{total => Size, filtered => length(Messages)}
                    }),
                    {ok, update_stream_state(Handler, SubId, StreamState), Messages};
                #status_unblocked{} ->
                    ok = suback(Handler, SubId, SubHandle, SeqNo),
                    Shard = get_shard_by_dsstream(StreamState0, DSStream),
                    LastTimestampUs0 = get_shard_start_time_us(StreamState0, Shard),
                    {LastTimestampUs, Messages} = to_messages(Shard, LastTimestampUs0, TTVs),
                    StreamState1 = advance_shard_last_time(StreamState0, Shard, LastTimestampUs),
                    StreamState = StreamState1#stream_state{status = #stream_status_unblocked{}},
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => unblocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => {messages, length(Messages)}
                    }),
                    {ok, update_stream_state(Handler, SubId, StreamState), Messages}
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

unblock_streams(#h{state = #state{subs = Subs}} = Handler) ->
    ?tp_debug(streams_extsub_handler_unblock_streams, #{}),
    maps:fold(
        fun
            (_SubId, #stream_state{status = #stream_status_unblocked{}}, HandlerAcc) ->
                HandlerAcc;
            (
                SubId,
                #stream_state{status = #stream_status_blocked{unblock_fns = UnblockFns}} =
                    StreamState0,
                HandlerAcc0
            ) ->
                HandlerAcc = unblock_stream(HandlerAcc0, UnblockFns),
                StreamState = StreamState0#stream_state{status = #stream_status_unblocked{}},
                update_stream_state(HandlerAcc, SubId, StreamState)
        end,
        Handler,
        Subs
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

remove_dsstream_from_index(#state{} = State, SubId, DSStream) ->
    #stream_state{shard_by_dsstream = ShardByDSStream} =
        StreamState0 = get_stream_state(SubId, State),
    StreamState = StreamState0#stream_state{
        shard_by_dsstream = maps:remove(DSStream, ShardByDSStream)
    },
    update_stream_state(State, SubId, StreamState).

complete_skipped_dsstream(
    #h{state = State0, ds_client = DSClient0} = Handler, SubId, Slab, DSStream
) ->
    {DSClient, State1} = emqx_ds_client:complete_stream(DSClient0, SubId, Slab, DSStream, State0),
    State = remove_dsstream_from_index(State1, SubId, DSStream),
    Handler#h{ds_client = DSClient, state = State}.

complete_subscribed_dsstream(
    #h{state = State0, ds_client = DSClient0} = Handler, SubId, SubRef, DSStream
) ->
    {DSClient, State1} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    State = remove_dsstream_from_index(State1, SubId, DSStream),
    Handler#h{ds_client = DSClient, state = State}.

add_dsstream_to_index(#state{} = State, SubId, DSStream, Shard) ->
    #stream_state{shard_by_dsstream = ShardByDSStream} =
        StreamState0 = get_stream_state(SubId, State),
    StreamState = StreamState0#stream_state{
        shard_by_dsstream = ShardByDSStream#{DSStream => Shard}
    },
    update_stream_state(State, SubId, StreamState).

get_shard_by_dsstream(#stream_state{shard_by_dsstream = ShardByDSStream}, DSStream) ->
    maps:get(DSStream, ShardByDSStream).

advance_shard_generation(
    #stream_state{shard = SubShard, start_time_us = StartTimeUs, progress = Progress0} =
        StreamState0,
    Shard,
    Generation
) when ?is_subscribed_to_shard(Shard, SubShard) ->
    ShardProgress =
        case Progress0 of
            #{Shard := #shard_progress{} = ShardProgress0} ->
                ShardProgress0#shard_progress{generation = Generation};
            _ ->
                #shard_progress{generation = Generation, last_timestamp_us = StartTimeUs}
        end,
    Progress = Progress0#{Shard => ShardProgress},
    StreamState0#stream_state{progress = Progress};
advance_shard_generation(StreamState0, _Shard, _Generation) ->
    StreamState0.

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

start_time_us(earliest) ->
    0;
start_time_us(latest) ->
    erlang:system_time(microsecond);
start_time_us(Timestamp) when is_integer(Timestamp) ->
    Timestamp.

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

validate_new_topic_filter(undefined, _TopicFilter) ->
    ok;
validate_new_topic_filter(
    #h{state = #state{by_topic_filter = TopicFilters, unknown_topic_filters = UnknownTopicFilters}},
    SubscribeTopicFilter
) ->
    case
        maps:is_key(SubscribeTopicFilter, TopicFilters) or
            sets:is_element(SubscribeTopicFilter, UnknownTopicFilters)
    of
        true -> ?err_unrec({topic_filter_already_present, #{topic_filter => SubscribeTopicFilter}});
        false -> ok
    end.

split_topic_filter(TopicFilter) ->
    case binary:split(TopicFilter, <<"/">>) of
        [First, Rest] -> {ok, First, Rest};
        _ -> ?err_unrec(invalid_topic_filter)
    end.

find_stream(TopicFilter) ->
    case emqx_streams_registry:find(TopicFilter) of
        {ok, Stream} ->
            {ok, Stream};
        not_found ->
            case emqx_streams_config:auto_create(TopicFilter) of
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
                    ?err_rec({stream_not_found, #{topic_filter => TopicFilter}})
            end
    end.

validate_partition(_Stream, <<"all">>) ->
    {ok, ?all_shards};
validate_partition(Stream, Partition) ->
    case string:to_integer(Partition) of
        {PartitionInt, <<>>} when PartitionInt >= 0 ->
            case lists:member(Partition, emqx_streams_message_db:partitions(Stream)) of
                true -> {ok, Partition};
                false -> ?err_rec({invalid_partition, #{partition => Partition}})
            end;
        _ ->
            ?err_unrec({invalid_partition, #{partition => Partition}})
    end.

%% Other helpers

to_messages(Shard, LastTimestampUs, TTVs) ->
    ?tp_debug(streams_extsub_handler_to_messages, #{
        shard => Shard, last_timestamp_us => LastTimestampUs, ttvs => TTVs
    }),
    {NewLastTimestampUs, Messages} = lists:foldl(
        fun({_Topic, Time, Payload}, {LastTimestampUsAcc, MessagesAcc}) ->
            case Time >= LastTimestampUsAcc of
                true -> {Time, [decode_message(Shard, Time, Payload) | MessagesAcc]};
                false -> {LastTimestampUsAcc, MessagesAcc}
            end
        end,
        {LastTimestampUs, []},
        TTVs
    ),
    {NewLastTimestampUs, lists:reverse(Messages)}.

suback(#h{state = #state{subs = Subs}}, SubId, SubHandle, SeqNo) ->
    case Subs of
        #{SubId := #stream_state{stream = Stream}} ->
            ok = emqx_streams_message_db:suback(Stream, SubHandle, SeqNo);
        _ ->
            ok
    end.

init_handler(undefined, #{send_after := SendAfterFn, send := SendFn} = _Ctx) ->
    State = #state{
        send_fn = SendFn,
        send_after_fn = SendAfterFn,
        status = #status_unblocked{},
        subs = #{},
        by_topic_filter = #{},
        unknown_topic_filters = sets:new([{version, 2}]),
        check_stream_status_tref = undefined
    },
    DSClient = emqx_streams_message_db:create_client(?MODULE),
    #h{state = State, ds_client = DSClient};
init_handler(#h{} = Handler, _Ctx) ->
    Handler.

update_stream_state(#h{state = State} = Handler, SubId, StreamState) ->
    Handler#h{state = update_stream_state(State, SubId, StreamState)};
update_stream_state(#state{subs = Subs} = State, SubId, StreamState) ->
    State#state{subs = Subs#{SubId => StreamState}}.

get_stream_state(SubId, #state{subs = Subs}) ->
    maps:get(SubId, Subs).

decode_message(Shard, Time, Payload) ->
    Message = emqx_streams_message_db:decode_message(Payload),
    add_properties(Message, [
        {<<"part">>, Shard},
        {<<"ts">>, integer_to_binary(Time)}
    ]).

add_properties(Message, UserProperties) when length(UserProperties) > 0 ->
    Props = emqx_message:get_header(properties, Message, #{}),
    UserProperties0 = maps:get('User-Property', Props, []),
    emqx_message:set_header(
        properties, Props#{'User-Property' => UserProperties0 ++ UserProperties}, Message
    ).

init_progress(Stream, ?all_shards, StartTimeUs) ->
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
    end;
init_progress(Stream, Shard, StartTimeUs) ->
    case emqx_streams_message_db:find_generation(Stream, Shard, StartTimeUs) of
        {ok, Generation} ->
            {ok, #{
                Shard => #shard_progress{generation = Generation, last_timestamp_us = StartTimeUs}
            }};
        {error, Reason} ->
            ?err_rec(
                {cannot_init_generations, #{stream => Stream, shard => Shard, reason => Reason}}
            )
    end.

%% Management of stream appearing/disappearing.

add_unknown_stream(
    #h{state = #state{unknown_topic_filters = UnknownTopicFilters0} = State0} = Handler, TopicFilter
) ->
    UnknownTopicFilters = sets:add_element(TopicFilter, UnknownTopicFilters0),
    State = State0#state{unknown_topic_filters = UnknownTopicFilters},
    Handler#h{state = State}.

schedule_check_stream_status(#h{state = State} = Handler) ->
    Handler#h{state = schedule_check_stream_status(State)};
schedule_check_stream_status(
    #state{unknown_topic_filters = UnknownTopicFilters, by_topic_filter = ByTopicFilter} = State
) ->
    schedule_check_stream_status(State, sets:size(UnknownTopicFilters) + maps:size(ByTopicFilter)).

schedule_check_stream_status(#state{check_stream_status_tref = TRef} = State, 0) ->
    _ = emqx_utils:cancel_timer(TRef),
    State#state{check_stream_status_tref = undefined};
schedule_check_stream_status(
    #state{check_stream_status_tref = undefined, send_after_fn = SendAfterFn} = State, N
) when N > 0 ->
    Interval = emqx_config:get([streams, check_stream_status_interval]),
    Ref = SendAfterFn(Interval, #check_stream_status{}),
    State#state{check_stream_status_tref = Ref};
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
    Handler1 = Handler0#h{state = State0#state{unknown_topic_filters = sets:new([{version, 2}])}},
    {Handler, UnknownTopicFilters} = lists:foldl(
        fun(SubscribeTopicFilter, {HandlerAcc0, UnknownTopicFiltersAcc}) ->
            case subscribe(HandlerAcc0, SubscribeTopicFilter) of
                {ok, HandlerAcc} ->
                    {HandlerAcc, UnknownTopicFiltersAcc};
                ?err_unrec(Reason) ->
                    ?tp(error, streams_extsub_handler_delayed_subscribe_error, #{
                        reason => Reason,
                        subscribe_topic_filter => SubscribeTopicFilter,
                        recoverable => false
                    }),
                    %% Should never happen, however, do not retry on unrecoverable error
                    {HandlerAcc0, UnknownTopicFiltersAcc};
                ?err_rec(Reason) ->
                    ?tp(info, streams_extsub_handler_delayed_subscribe_error, #{
                        reason => Reason,
                        subscribe_topic_filter => SubscribeTopicFilter,
                        recoverable => true
                    }),
                    {HandlerAcc0, sets:add_element(SubscribeTopicFilter, UnknownTopicFiltersAcc)}
            end
        end,
        {Handler1, sets:new([{version, 2}])},
        sets:to_list(UnknownTopicFilters0)
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
        fun(SubscribeTopicFilter, SubId, {HandlerAcc0, UnknownTopicFiltersAcc}) ->
            #h{state = #state{subs = Subs}} = HandlerAcc0,
            #stream_state{stream = #{id := Id} = Stream} = maps:get(SubId, Subs),
            case emqx_streams_registry:find(emqx_streams_prop:topic_filter(Stream)) of
                {ok, #{id := Id}} ->
                    {HandlerAcc0, UnknownTopicFiltersAcc};
                _ ->
                    HandlerAcc = unsubscribe(HandlerAcc0, SubscribeTopicFilter, SubId),
                    {HandlerAcc, sets:add_element(SubscribeTopicFilter, UnknownTopicFiltersAcc)}
            end
        end,
        {Handler0, UnknownTopicFilters0},
        ByTopicFilter
    ),
    #h{state = State} = Handler,
    Handler#h{state = State#state{unknown_topic_filters = UnknownTopicFilters}}.
