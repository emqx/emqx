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
    status :: status(),
    subs :: #{sub_id() => stream_state()},
    topic_filters :: #{emqx_types:topic() => sub_id()}
}).

-type state() :: #state{}.

-record(h, {
    state :: state(),
    ds_client :: emqx_ds_client:client()
}).

-type handler() :: #h{}.

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
    <<"$s/", Rest0/binary>> = FullTopicFilter
) ->
    maybe
        {ok, Partition, Rest1} ?= split_topic_filter(Rest0),
        {ok, OffsetBin, TopicFilter} ?= split_topic_filter(Rest1),
        {ok, Stream} ?= find_stream(TopicFilter),
        {ok, Shard} ?= validate_partition(Stream, Partition),
        {ok, Offset} ?= parse_offset(OffsetBin),
        ok ?= validate_new_topic_filter(Handler0, FullTopicFilter),
        #h{
            state = #state{topic_filters = TopicFilters, subs = Subs} = State0,
            ds_client = DSClient0
        } = Handler = init_handler(Handler0, SubscribeCtx),
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
            topic_filters = TopicFilters#{FullTopicFilter => SubId},
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
    else
        {error, Error} ->
            ?tp(error, streams_extsub_handler_subscribe_error, #{
                error => Error, topic_filter => FullTopicFilter
            }),
            ignore
    end;
handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, _FullTopicFilter) ->
    ignore.

handle_unsubscribe(
    _UnsubscribeType,
    #h{state = #state{topic_filters = TopicFilters0} = State0, ds_client = DSClient0} = Handler,
    FullTopicFilter
) ->
    case TopicFilters0 of
        #{FullTopicFilter := SubId} ->
            {ok, DSClient, #state{topic_filters = TopicFilters, subs = Subs} = State1} = emqx_ds_client:unsubscribe(
                DSClient0, SubId, State0
            ),
            State = State1#state{
                topic_filters = maps:remove(FullTopicFilter, TopicFilters),
                subs = maps:remove(SubId, Subs)
            },
            Handler#h{state = State, ds_client = DSClient};
        _ ->
            Handler
    end.

handle_terminate(#h{state = State, ds_client = DSClient}) ->
    ?tp(debug, handle_terminate, #{state => State}),
    _ = emqx_ds_client:destroy(DSClient, State),
    ok.

handle_delivered(
    HState,
    #{desired_message_count := DesiredCount} = _DeliveredCtx,
    _Message,
    _Ack
) ->
    update_blocking_status(HState, DesiredCount).

handle_info(HState, #{desired_message_count := DesiredCount} = _InfoCtx, Info) ->
    handle_info(update_blocking_status(HState, DesiredCount), Info).

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
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = HState, {generic, Info}
) ->
    case emqx_ds_client:dispatch_message(Info, DSClient0, State0) of
        ignore ->
            {ok, HState};
        {DSClient, State} ->
            {ok, HState#h{ds_client = DSClient, state = State}};
        {data, SubId, DSStream, SubHandle, DSReply} ->
            case Subs of
                #{SubId := StreamState} ->
                    handle_ds_info(HState, StreamState, SubId, DSStream, SubHandle, DSReply);
                _ ->
                    {ok, HState}
            end
    end;
handle_info(
    #h{state = #state{subs = Subs} = State0, ds_client = DSClient0} = HState0, #complete_stream{
        sub_id = SubId, slab = Slab, ds_stream = DSStream
    }
) ->
    case Subs of
        #{SubId := _} ->
            {DSClient, State} = emqx_ds_client:complete_stream(
                DSClient0, SubId, Slab, DSStream, State0
            ),
            {ok, HState0#h{ds_client = DSClient, state = State}};
        _ ->
            {ok, HState0}
    end.

handle_ds_info(
    #h{state = #state{status = Status}} = HState, StreamState0, SubId, _DSStream, SubHandle, DSReply
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
                    {ok, update_stream_state(HState, SubId, StreamState)};
                #status_unblocked{} ->
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => unblocked,
                        sub_id => SubId,
                        stream_state => StreamState0,
                        payload => end_of_stream
                    }),
                    {ok, complete_ds_stream(HState, SubRef)}
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
                    {ok, update_stream_state(HState, SubId, StreamState), Messages};
                #status_unblocked{} ->
                    ok = suback(HState, SubId, SubHandle, SeqNo),
                    {LastTimestampUs, Messages} = to_messages(StreamState0, TTVs),
                    StreamState = StreamState0#stream_state{start_time_us = LastTimestampUs},
                    ?tp_debug(streams_extsub_handler_handle_ds_info, #{
                        status => unblocked,
                        sub_id => SubId,
                        stream_state => StreamState,
                        payload => {messages, length(Messages)}
                    }),
                    {ok, update_stream_state(HState, SubId, StreamState), Messages}
            end
    end.

update_blocking_status(#h{state = #state{status = Status} = State0} = HState0, DesiredCount) ->
    case {Status, DesiredCount} of
        {#status_unblocked{}, 0} ->
            HState0#h{state = State0#state{status = #status_blocked{}}};
        {#status_unblocked{}, N} when N > 0 ->
            HState0;
        {#status_blocked{}, 0} ->
            HState0;
        {#status_blocked{}, N} when N > 0 ->
            HState = #h{state = State} = unblock_streams(HState0),
            HState#h{state = State#state{status = #status_unblocked{}}}
    end.

unblock_streams(#h{state = #state{subs = Subs}} = HState) ->
    ?tp_debug(streams_extsub_handler_unblock_streams, #{}),
    maps:fold(
        fun
            (_SubId, #stream_state{status = #stream_status_unblocked{}}, HStateAcc) ->
                HStateAcc;
            (
                SubId,
                #stream_state{status = #stream_status_blocked{unblock_fn = UnblockFn}} =
                    StreamState0,
                HStateAcc0
            ) ->
                HStateAcc = UnblockFn(HStateAcc0),
                StreamState = StreamState0#stream_state{status = #stream_status_unblocked{}},
                update_stream_state(HStateAcc, SubId, StreamState)
        end,
        HState,
        Subs
    ).

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

complete_ds_stream(#h{state = State0, ds_client = DSClient0} = HState, SubRef) ->
    {DSClient, State} = emqx_ds_client:complete_stream(DSClient0, SubRef, State0),
    HState#h{ds_client = DSClient, state = State}.

suback(#h{state = #state{subs = Subs}}, SubId, SubHandle, SeqNo) ->
    case Subs of
        #{SubId := #stream_state{stream = Stream}} ->
            ok = emqx_streams_message_db:suback(Stream, SubHandle, SeqNo);
        _ ->
            ok
    end.

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
            {error, invalid_offset}
    end.

init_handler(undefined, #{send_after := SendAfterFn, send := SendFn} = _Ctx) ->
    State = #state{
        send_fn = SendFn,
        send_after_fn = SendAfterFn,
        status = #status_unblocked{},
        subs = #{},
        topic_filters = #{}
    },
    DSClient = emqx_streams_message_db:create_client(?MODULE),
    #h{state = State, ds_client = DSClient};
init_handler(#h{} = Handler, _Ctx) ->
    Handler.

validate_new_topic_filter(undefined, _TopicFilter) ->
    ok;
validate_new_topic_filter(#h{state = #state{topic_filters = TopicFilters}}, FullTopicFilter) ->
    case maps:is_key(FullTopicFilter, TopicFilters) of
        true -> {error, {topic_filter_already_present, FullTopicFilter}};
        false -> ok
    end.

update_stream_state(#h{state = State} = HState, SubId, StreamState) ->
    HState#h{state = update_stream_state(State, SubId, StreamState)};
update_stream_state(#state{subs = Subs} = State, SubId, StreamState) ->
    State#state{subs = Subs#{SubId => StreamState}}.

start_time_us(earliest) ->
    0;
start_time_us(latest) ->
    erlang:system_time(microsecond);
start_time_us(Timestamp) when is_integer(Timestamp) ->
    Timestamp.

split_topic_filter(TopicFilter) ->
    case binary:split(TopicFilter, <<"/">>) of
        [First, Rest] -> {ok, First, Rest};
        _ -> {error, invalid_topic_filter}
    end.

find_stream(TopicFilter) ->
    case emqx_streams_registry:find(TopicFilter) of
        {ok, Stream} ->
            {ok, Stream};
        not_found ->
            case emqx_streams_config:auto_create(TopicFilter) of
                {true, DefaultStream} ->
                    % {ok, Stream};
                    case emqx_streams_registry:create(DefaultStream) of
                        {ok, Stream} ->
                            {ok, Stream};
                        {error, Reason} ->
                            {error,
                                {cannot_auto_create_stream, #{
                                    stream => DefaultStream, reason => Reason
                                }}}
                    end;
                false ->
                    {error, {stream_not_found, TopicFilter}}
            end
    end.

validate_partition(Stream, Partition) ->
    case lists:member(Partition, emqx_streams_message_db:partitions(Stream)) of
        true -> {ok, Partition};
        false -> {error, {invalid_partition, Partition}}
    end.

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
