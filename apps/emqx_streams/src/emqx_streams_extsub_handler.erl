%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-type ds_sub_id() :: reference().

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
    ds_sub_id :: ds_sub_id(),
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
    %% "$s/earliest/t/#" and "$sp/0/latest/t/#".
    ds_subs :: #{ds_sub_id() => stream_state()},
    by_topic_filter :: #{emqx_types:topic() => ds_sub_id()},
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
    ds_sub_id :: ds_sub_id(),
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
    <<"$s/", NoPartitionSubscribeTopicFilter/binary>>
) ->
    do_handle_subscribe(SubscribeCtx, Handler0, <<"all/", NoPartitionSubscribeTopicFilter/binary>>);
handle_subscribe(
    _SubscribeType,
    SubscribeCtx,
    Handler0,
    <<"$sp/", SubscribeTopicFilter/binary>>
) ->
    do_handle_subscribe_partition(SubscribeCtx, Handler0, SubscribeTopicFilter);
handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, _SubscribeTopicFilter) ->
    ignore.

%% Hide partitions from the user for now
-ifdef(TEST).

do_handle_subscribe_partition(SubscribeCtx, Handler0, SubscribeTopicFilter) ->
    do_handle_subscribe(SubscribeCtx, Handler0, SubscribeTopicFilter).

-else.

do_handle_subscribe_partition(_SubscribeCtx, _Handler0, _SubscribeTopicFilter) ->
    ignore.

-endif.

do_handle_subscribe(SubscribeCtx, Handler0, SubscribeTopicFilter) ->
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
    end.

handle_unsubscribe(_UnsubscribeType, Handler, <<"$s/", SubscribeTopicFilter/binary>>) ->
    unsubscribe(Handler, <<"all/", SubscribeTopicFilter/binary>>);
handle_unsubscribe(_UnsubscribeType, Handler, <<"$sp/", SubscribeTopicFilter/binary>>) ->
    unsubscribe(Handler, SubscribeTopicFilter);
handle_unsubscribe(_UnsubscribeType, Handler, SubscribeTopicFilter) ->
    ?tp(error, streams_extsub_handler_unsubscribe_unknown_topic_filter, #{
        unsubscribe_topic_filter => SubscribeTopicFilter
    }),
    Handler.

unsubscribe(
    #h{
        state =
            #state{by_topic_filter = ByTopicFilter0, unknown_topic_filters = UnknownTopicFilters} =
                State0
    } = Handler,
    SubscribeTopicFilter
) ->
    case ByTopicFilter0 of
        #{SubscribeTopicFilter := DSSubId} ->
            unsubscribe(Handler, SubscribeTopicFilter, DSSubId);
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

get_iterator(DSSubId, {Shard, _} = Slab, DSStream, #state{ds_subs = DSSubs, send_fn = SendFn}) ->
    case DSSubs of
        #{DSSubId := #stream_state{shard = SubShard} = StreamState} when
            ?is_subscribed_to_shard(Shard, SubShard)
        ->
            StartTimeUs = get_shard_start_time_us(StreamState, Shard),
            {undefined, #{start_time => StartTimeUs}};
        #{DSSubId := _} ->
            %% Other shards we are not interested in
            SendFn(#complete_stream{
                ds_sub_id = DSSubId, slab = Slab, ds_stream = DSStream
            }),
            {ok, end_of_stream};
        _ ->
            %% Should never happen
            error({unknown_subscription, #{ds_sub_id => DSSubId}})
    end.

on_new_iterator(
    DSSubId, {Shard, _} = Slab, DSStream, _It, #state{ds_subs = DSSubs, send_fn = SendFn} = State
) ->
    case DSSubs of
        #{DSSubId := #stream_state{shard = SubShard}} when
            SubShard =:= Shard orelse SubShard =:= ?all_shards
        ->
            {subscribe, add_dsstream_to_index(State, DSSubId, DSStream, Shard)};
        #{DSSubId := _} ->
            SendFn(#complete_stream{
                ds_sub_id = DSSubId, slab = Slab, ds_stream = DSStream
            }),
            {ignore, State};
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

subscribe(Handler, SubscribeTopicFilter) ->
    maybe
        {ok, Partition, Rest1} ?= split_topic_filter(SubscribeTopicFilter),
        {ok, OffsetBin, TopicFilter} ?= split_topic_filter(Rest1),
        {ok, Offset} ?= parse_offset(OffsetBin),
        ok ?= validate_new_topic_filter(Handler, SubscribeTopicFilter),
        {ok, Stream} ?= find_stream(TopicFilter),
        {ok, SubShard} ?= validate_partition(Stream, Partition),
        #h{
            state = #state{by_topic_filter = ByTopicFilter, ds_subs = DSSubs} = State0,
            ds_client = DSClient0
        } = Handler,
        DSSubId = make_ref(),
        StartTimeUs = start_time_us(Offset),
        {ok, Progress} ?= init_progress(Stream, SubShard, StartTimeUs),
        StreamState = #stream_state{
            stream = Stream,
            shard = SubShard,
            start_time_us = StartTimeUs,
            progress = Progress,
            shard_by_dsstream = #{},
            ds_sub_id = DSSubId,
            status = #stream_status_unblocked{}
        },
        State1 = State0#state{
            by_topic_filter = ByTopicFilter#{SubscribeTopicFilter => DSSubId},
            ds_subs = DSSubs#{DSSubId => StreamState}
        },
        {ok, DSClient, State} = emqx_streams_message_db:subscribe(
            Stream, DSClient0, DSSubId, State1
        ),
        {ok, Handler#h{state = State, ds_client = DSClient}}
    end.

unsubscribe(
    #h{
        state = #state{by_topic_filter = ByTopicFilter, ds_subs = DSSubs} = State0,
        ds_client = DSClient0
    } =
        Handler,
    SubscribeTopicFilter,
    DSSubId
) ->
    {ok, DSClient, #state{by_topic_filter = ByTopicFilter, ds_subs = DSSubs} = State1} = emqx_ds_client:unsubscribe(
        DSClient0, DSSubId, State0
    ),
    State = State1#state{
        by_topic_filter = maps:remove(SubscribeTopicFilter, ByTopicFilter),
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
handle_info(
    #h{state = #state{ds_subs = DSSubs}} = Handler, #complete_stream{
        ds_sub_id = DSSubId, slab = Slab, ds_stream = DSStream
    }
) ->
    case DSSubs of
        #{DSSubId := _} ->
            {ok, complete_skipped_dsstream(Handler, DSSubId, Slab, DSStream)};
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

remove_dsstream_from_index(#state{} = State, DSSubId, DSStream) ->
    #stream_state{shard_by_dsstream = ShardByDSStream} =
        StreamState0 = get_stream_state(DSSubId, State),
    StreamState = StreamState0#stream_state{
        shard_by_dsstream = maps:remove(DSStream, ShardByDSStream)
    },
    update_stream_state(State, DSSubId, StreamState).

complete_skipped_dsstream(
    #h{state = State0, ds_client = DSClient0} = Handler, DSSubId, Slab, DSStream
) ->
    {DSClient, State1} = emqx_ds_client:complete_stream(DSClient0, DSSubId, Slab, DSStream, State0),
    State = remove_dsstream_from_index(State1, DSSubId, DSStream),
    Handler#h{ds_client = DSClient, state = State}.

complete_subscribed_dsstream(
    #h{state = State0, ds_client = DSClient0} = Handler, DSSubId, SubRef, DSStream
) ->
    {DSClient, State1} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    State = remove_dsstream_from_index(State1, DSSubId, DSStream),
    Handler#h{ds_client = DSClient, state = State}.

add_dsstream_to_index(#state{} = State, DSSubId, DSStream, Shard) ->
    #stream_state{shard_by_dsstream = ShardByDSStream} =
        StreamState0 = get_stream_state(DSSubId, State),
    StreamState = StreamState0#stream_state{
        shard_by_dsstream = ShardByDSStream#{DSStream => Shard}
    },
    update_stream_state(State, DSSubId, StreamState).

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

-dialyzer([{nowarn_function, validate_partition/2}]).
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
        unknown_topic_filters = sets:new([{version, 2}]),
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

get_stream_state(DSSubId, #state{ds_subs = DSSubs}) ->
    maps:get(DSSubId, DSSubs).

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

inc_received_message_stat(#ds_sub_reply{payload = {ok, _It, _TTVs}, size = Size}) ->
    emqx_streams_metrics:inc(ds, received_messages, Size);
inc_received_message_stat(#ds_sub_reply{}) ->
    ok.

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
        fun(SubscribeTopicFilter, DSSubId, {HandlerAcc0, UnknownTopicFiltersAcc}) ->
            #h{state = #state{ds_subs = DSSubs}} = HandlerAcc0,
            #stream_state{stream = #{id := Id} = Stream} = maps:get(DSSubId, DSSubs),
            case emqx_streams_registry:find(emqx_streams_prop:topic_filter(Stream)) of
                {ok, #{id := Id}} ->
                    {HandlerAcc0, UnknownTopicFiltersAcc};
                _ ->
                    HandlerAcc = unsubscribe(HandlerAcc0, SubscribeTopicFilter, DSSubId),
                    {HandlerAcc, sets:add_element(SubscribeTopicFilter, UnknownTopicFiltersAcc)}
            end
        end,
        {Handler0, UnknownTopicFilters0},
        ByTopicFilter
    ),
    #h{state = State} = Handler,
    Handler#h{state = State#state{unknown_topic_filters = UnknownTopicFilters}}.
