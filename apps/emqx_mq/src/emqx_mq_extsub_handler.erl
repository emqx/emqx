%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_extsub_handler).

-moduledoc """
Multi-topic ExtSub handler for Message Queue subscriptions.

One handler instance per client manages ALL of that client's MQ subscriptions.
The consumer sends messages to subscriber_ref (a channel alias), which arrive
as generic messages. The handler dispatches them to the correct emqx_mq_sub
by subscriber_ref, then returns the messages to emqx_extsub for buffering
and delivery.
""".

-behaviour(emqx_extsub_handler).

-include("emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% ExtSub handler callbacks
-export([
    handle_subscribe/4,
    handle_unsubscribe/4,
    handle_terminate/1,
    handle_delivered/4,
    handle_info/3
]).

-record(sub, {
    tf :: emqx_extsub_types:topic_filter(),
    sub :: emqx_mq_sub:t()
}).

-record(state, {
    %% subscriber_ref -> #sub{} (full topic_filter + mq_sub state)
    subs :: #{emqx_mq_types:subscriber_ref() => #sub{}},
    %% topic_filter (full, e.g. <<"$queue/myq/t/#">>) -> subscriber_ref
    by_topic :: #{emqx_extsub_types:topic_filter() => emqx_mq_types:subscriber_ref()}
}).

%%--------------------------------------------------------------------
%% ExtSub handler callbacks
%%--------------------------------------------------------------------

handle_subscribe(_SubscribeType, SubscribeCtx, Handler0, TopicFilter) ->
    Handler1 = init_handler(Handler0),
    case check_mq_topic_filter(SubscribeCtx, TopicFilter) of
        {ok, Name, MQTopic} ->
            #{clientinfo := ClientInfo} = SubscribeCtx,
            ok = maybe_auto_create(Name, MQTopic),
            Sub = emqx_mq_sub:handle_connect(ClientInfo, Name, MQTopic),
            SubscriberRef = emqx_mq_sub:subscriber_ref(Sub),
            #state{subs = Subs, by_topic = ByTopic} = Handler1,
            {ok, Handler1#state{
                subs = Subs#{SubscriberRef => #sub{tf = TopicFilter, sub = Sub}},
                by_topic = ByTopic#{TopicFilter => SubscriberRef}
            }};
        ignore ->
            ignore;
        {error, Reason} ->
            ?tp(warning, mq_extsub_handler_subscribe_error, #{
                reason => Reason, topic_filter => TopicFilter
            }),
            ignore
    end.

handle_unsubscribe(_UnsubscribeType, _UnsubscribeCtx, Handler, TopicFilter) ->
    #state{subs = Subs, by_topic = ByTopic} = Handler,
    case ByTopic of
        #{TopicFilter := SubscriberRef} ->
            #{SubscriberRef := #sub{sub = Sub}} = Subs,
            ok = emqx_mq_sub:handle_disconnect(Sub),
            Handler#state{
                subs = maps:remove(SubscriberRef, Subs),
                by_topic = maps:remove(TopicFilter, ByTopic)
            };
        _ ->
            Handler
    end.

handle_terminate(#state{subs = Subs}) ->
    maps:foreach(fun(_Ref, #sub{sub = Sub}) -> ok = emqx_mq_sub:handle_disconnect(Sub) end, Subs),
    ok.

handle_delivered(
    #state{subs = Subs} = Handler,
    _AckCtx,
    Msg,
    Ack
) ->
    SubscriberRef = emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined),
    MessageId = emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg, undefined),
    case SubscriberRef =:= undefined orelse MessageId =:= undefined of
        true ->
            {ok, Handler};
        false ->
            case Subs of
                #{SubscriberRef := #sub{sub = Sub}} ->
                    MqAck = mq_ack_from_extsub_ack(Ack),
                    ok = emqx_mq_sub:ack_consumer(Sub, MessageId, MqAck),
                    {ok, Handler};
                _ ->
                    ?tp_debug(mq_on_delivery_completed_sub_not_found, #{
                        subscriber_ref => SubscriberRef
                    }),
                    {ok, Handler}
            end
    end.

handle_info(
    Handler,
    #{clientinfo := ClientInfo},
    {generic, #info_to_mq_sub{subscriber_ref = SubscriberRef, info = Info}}
) ->
    dispatch_to_sub(Handler, ClientInfo, SubscriberRef, Info);
handle_info(
    #state{subs = Subs} = Handler,
    _InfoCtx,
    {generic, #info_mq_inspect{receiver = Receiver, name = Name, topic_filter = Topic}}
) ->
    Info =
        case find_sub_by_name_topic(Subs, Name, Topic) of
            [] -> undefined;
            [Sub] -> emqx_mq_sub:inspect(Sub)
        end,
    erlang:send(Receiver, {Receiver, Info}),
    {ok, Handler};
handle_info(Handler, _InfoCtx, _Info) ->
    {ok, Handler}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_handler(undefined) ->
    #state{
        subs = #{},
        by_topic = #{}
    };
init_handler(#state{} = Handler) ->
    Handler.

dispatch_to_sub(
    #state{subs = Subs, by_topic = ByTopic} = Handler,
    ClientInfo,
    SubscriberRef,
    Info
) ->
    case Subs of
        #{SubscriberRef := #sub{tf = FullTopicFilter, sub = Sub0}} ->
            case emqx_mq_sub:handle_info(Sub0, Info) of
                {ok, Sub} ->
                    SubRec = #sub{tf = FullTopicFilter, sub = Sub},
                    {ok, Handler#state{subs = Subs#{SubscriberRef => SubRec}}};
                {ok, Sub, Messages} ->
                    SubRec = #sub{tf = FullTopicFilter, sub = Sub},
                    EnrichedMsgs = enrich_messages(SubscriberRef, FullTopicFilter, Messages),
                    {ok, Handler#state{subs = Subs#{SubscriberRef => SubRec}}, EnrichedMsgs};
                {error, recreate} ->
                    {Name, MQTopic} = emqx_mq_sub:name_topic(Sub0),
                    ok = emqx_mq_sub:handle_disconnect(Sub0),
                    NewSub = emqx_mq_sub:handle_connect(ClientInfo, Name, MQTopic),
                    NewRef = emqx_mq_sub:subscriber_ref(NewSub),
                    Subs1 = maps:remove(SubscriberRef, Subs),
                    {ok, Handler#state{
                        subs = Subs1#{NewRef => #sub{tf = FullTopicFilter, sub = NewSub}},
                        by_topic = ByTopic#{FullTopicFilter => NewRef}
                    }}
            end;
        _ ->
            {ok, Handler}
    end.

enrich_messages(SubscriberRef, FullTopicFilter, Messages) ->
    lists:map(
        fun(Msg0) ->
            Msg1 = emqx_message:set_headers(
                #{
                    ?MQ_HEADER_SUBSCRIBER_ID => SubscriberRef,
                    ?HEADER_SUB_TOPIC => FullTopicFilter
                },
                Msg0
            ),
            Msg1#message{qos = ?QOS_1}
        end,
        Messages
    ).

find_sub_by_name_topic(Subs, Name, Topic) ->
    lists:filtermap(
        fun(#sub{sub = Sub}) ->
            case emqx_mq_sub:name_topic(Sub) of
                {Name, Topic} -> {true, Sub};
                _ -> false
            end
        end,
        maps:values(Subs)
    ).

check_mq_topic_filter(Ctx, <<"$queue/", NameTopicBin/binary>> = _FullTopic) ->
    case validate_protocol(Ctx) of
        ok -> split_name_topic(NameTopicBin);
        {error, _} = Err -> Err
    end;
check_mq_topic_filter(Ctx, <<"$q/", TopicFilter/binary>>) ->
    case validate_protocol(Ctx) of
        ok ->
            Name = emqx_mq_prop:default_name_from_topic(TopicFilter),
            {ok, Name, TopicFilter};
        {error, _} = Err ->
            Err
    end;
check_mq_topic_filter(_Ctx, _TopicFilter) ->
    ignore.

validate_protocol(#{conninfo_fn := ConnInfoFn, clientinfo := ClientInfo}) ->
    ProtoVer = ConnInfoFn(proto_ver),
    Protocol = maps:get(protocol, ClientInfo, undefined),
    case {Protocol, ProtoVer} of
        {mqtt, ?MQTT_PROTO_V5} ->
            ok;
        Unsupported ->
            {error, {mq_not_supported_for_protocol, Unsupported}}
    end.

split_name_topic(NameTopic) ->
    case binary:split(NameTopic, <<"/">>) of
        [Name, Topic] ->
            ok;
        _ ->
            Name = NameTopic,
            Topic = undefined
    end,
    case emqx_mq_schema:validate_name(Name) of
        ok -> {ok, Name, Topic};
        {error, _} = Err -> Err
    end.

maybe_auto_create(_Name, undefined) ->
    ok;
maybe_auto_create(Name, Topic) ->
    case emqx_mq_config:auto_create(Name, Topic) of
        {true, MQ} -> auto_create_mq(MQ);
        false -> ok
    end.

auto_create_mq(#{name := Name} = MQ) ->
    case emqx_mq_registry:find(Name) of
        {ok, _} ->
            ok;
        not_found ->
            case emqx_mq_registry:create(MQ) of
                {ok, _} ->
                    ok;
                {error, {queue_exists, _}} ->
                    ok;
                {error, Reason} ->
                    ?tp(error, mq_extsub_handler_auto_create_error, #{
                        mq => MQ, reason => Reason
                    }),
                    ok
            end
    end.

mq_ack_from_extsub_ack(undefined) -> ?MQ_ACK;
mq_ack_from_extsub_ack(?RC_SUCCESS) -> ?MQ_ACK;
mq_ack_from_extsub_ack(_) -> ?MQ_REJECTED.
