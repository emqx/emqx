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
    unblock_fn :: fun((handler()) -> handler())
}).

-type stream_status() :: #stream_status_unblocked{} | #stream_status_blocked{}.

-record(status_blocked, {}).
-record(status_unblocked, {}).

-type status() :: #status_blocked{} | #status_unblocked{}.

%% DS Client subscription ID
-type sub_id() :: reference().

-record(stream_state, {
    stream :: emqx_streams_types:stream(),
    shard :: emqx_ds:shard(),
    generation :: emqx_ds:generation(),
    start_time_us :: emqx_ds:time(),
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
%% API
%%------------------------------------------------------------------------------------

%% ExtSub Handler callbacks

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

subscribe(Handler, SubscribeTopicFilter) ->
    maybe
        {ok, Partition, Rest1} ?= split_topic_filter(SubscribeTopicFilter),
        {ok, OffsetBin, TopicFilter} ?= split_topic_filter(Rest1),
        {ok, Offset} ?= parse_offset(OffsetBin),
        ok ?= validate_new_topic_filter(Handler, SubscribeTopicFilter),
        {ok, Stream} ?= find_stream(TopicFilter),
        {ok, Shard} ?= validate_partition(Stream, Partition),
        #h{
            state = #state{by_topic_filter = ByTopicFilter, subs = Subs} = State0,
            ds_client = DSClient0
        } = Handler,
        SubId = make_ref(),
        StartTimeUs = start_time_us(Offset),
        {ok, Generation} ?= emqx_streams_message_db:find_generation(Stream, Shard, StartTimeUs),
        StreamState = #stream_state{
            stream = Stream,
            shard = Shard,
            generation = Generation,
            start_time_us = StartTimeUs,
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
            shard => Shard,
            generation => Generation,
            start_time_us => StartTimeUs
        }),
        {ok, DSClient, State} = emqx_streams_message_db:subscribe(Stream, DSClient0, SubId, State1),
        {ok, Handler#h{state = State, ds_client = DSClient}}
    end.

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

%% DS Client callbacks

get_current_generation(SubId, _Shard, #state{subs = Subs}) ->
    ?tp_debug(streams_extsub_handler_get_current_generation, #{sub_id => SubId, shard => _Shard}),
    case Subs of
        #{SubId := #stream_state{generation = Generation}} ->
            Generation;
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
        #{SubId := #stream_state{shard = Shard} = StreamState} ->
            update_stream_state(
                State,
                SubId,
                StreamState#stream_state{generation = Generation}
            );
        #{SubId := _} ->
            State;
        _ ->
            error({unknown_subscription, #{sub_id => SubId}})
    end.

get_iterator(SubId, {Shard, _} = Slab, DSStream, #state{subs = Subs, send_fn = SendFn}) ->
    case Subs of
        #{SubId := #stream_state{shard = Shard, start_time_us = StartTimeUs}} ->
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
        #{SubId := #stream_state{shard = Shard}} ->
            ?tp_debug(streams_extsub_handler_on_new_iterator_subscribe, #{
                status => subscribe,
                sub_id => SubId,
                slab => Slab,
                ds_stream => DSStream
            }),
            {subscribe, State};
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

%% Internal functions

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
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = Handler0, #complete_stream{
        sub_id = SubId, slab = Slab, ds_stream = DSStream
    }
) ->
    case Subs of
        #{SubId := _} ->
            {DSClient, State} = emqx_ds_client:complete_stream(
                DSClient0, SubId, Slab, DSStream, State0
            ),
            {ok, Handler0#h{ds_client = DSClient, state = State}};
        _ ->
            {ok, Handler0}
    end;
handle_info(Handler0, #check_stream_status{}) ->
    Handler1 = check_active_streams_status(Handler0),
    Handler = check_unknown_stream_status(Handler1),
    {ok, reschedule_check_stream_status(Handler)}.

handle_ds_info(
    #h{state = #state{status = Status}} = Handler,
    StreamState0,
    SubId,
    _DSStream,
    SubHandle,
    DSReply
) ->
    case DSReply of
        #ds_sub_reply{payload = {ok, end_of_stream}, ref = SubRef} ->
            case Status of
                #status_blocked{} ->
                    StreamStatus = #stream_status_blocked{
                        unblock_fn = fun(HSt) ->
                            complete_ds_stream(HSt, SubRef)
                        end
                    },
                    StreamState = StreamState0#stream_state{status = StreamStatus},
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
                    {ok, complete_ds_stream(Handler, SubRef)}
            end;
        #ds_sub_reply{payload = {ok, _It, TTVs}, seqno = SeqNo, size = _Size} ->
            case Status of
                #status_blocked{} ->
                    StreamStatus = #stream_status_blocked{
                        unblock_fn = fun(HSt) ->
                            ok = suback(HSt, SubId, SubHandle, SeqNo),
                            HSt
                        end
                    },
                    {LastTimestampUs, Messages} = to_messages(StreamState0, TTVs),
                    StreamState = StreamState0#stream_state{
                        status = StreamStatus, start_time_us = LastTimestampUs
                    },
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => blocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => {messages, length(Messages)}
                    }),
                    {ok, update_stream_state(Handler, SubId, StreamState), Messages};
                #status_unblocked{} ->
                    ok = suback(Handler, SubId, SubHandle, SeqNo),
                    {LastTimestampUs, Messages} = to_messages(StreamState0, TTVs),
                    StreamState = StreamState0#stream_state{start_time_us = LastTimestampUs},
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => unblocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => {messages, length(Messages)}
                    }),
                    {ok, update_stream_state(Handler, SubId, StreamState), Messages}
            end
    end.

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
                #stream_state{status = #stream_status_blocked{unblock_fn = UnblockFn}} =
                    StreamState0,
                HandlerAcc0
            ) ->
                HandlerAcc = UnblockFn(HandlerAcc0),
                StreamState = StreamState0#stream_state{status = #stream_status_unblocked{}},
                update_stream_state(HandlerAcc, SubId, StreamState)
        end,
        Handler,
        Subs
    ).

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

to_messages(#stream_state{start_time_us = StartTimeUs} = StreamState, TTVs) ->
    {NewLastTimestampUs, Messages} = lists:foldl(
        fun({_Topic, Time, Payload}, {LastTimestampUsAcc, MessagesAcc}) ->
            case Time >= LastTimestampUsAcc of
                true -> {Time, [decode_message(StreamState, Time, Payload) | MessagesAcc]};
                false -> {LastTimestampUsAcc, MessagesAcc}
            end
        end,
        {StartTimeUs, []},
        TTVs
    ),
    {NewLastTimestampUs, lists:reverse(Messages)}.

complete_ds_stream(#h{state = State0, ds_client = DSClient0} = Handler, SubRef) ->
    {DSClient, State} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    Handler#h{ds_client = DSClient, state = State}.

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

decode_message(#stream_state{shard = Shard}, Time, Payload) ->
    Message = emqx_streams_message_db:decode_message(Payload),
    add_properties(Message, [
        {<<"part">>, Shard},
        {<<"offset">>, integer_to_binary(Time)}
    ]).

add_properties(Message, UserProperties) when length(UserProperties) > 0 ->
    Props = emqx_message:get_header(properties, Message, #{}),
    UserProperties0 = maps:get('User-Property', Props, []),
    emqx_message:set_header(
        properties, Props#{'User-Property' => UserProperties0 ++ UserProperties}, Message
    ).

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
