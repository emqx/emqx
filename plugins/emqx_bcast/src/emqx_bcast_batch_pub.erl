%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_batch_pub).

-export([handle/2]).

-include("emqx_bcast.hrl").

handle(Body, RequestId) ->
    ProductKey = maps:get(<<"ProductKey">>, Body, undefined),
    DeviceNames = maps:get(<<"DeviceName">>, Body, undefined),
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    MessageId = maps:get(<<"MessageId">>, Body, undefined),
    Qos = maps:get(<<"Qos">>, Body, 0),
    TopicShortName = maps:get(<<"TopicShortName">>, Body, undefined),
    TopicTemplateName = maps:get(<<"TopicTemplateName">>, Body, undefined),

    case validate_input(ProductKey, DeviceNames, MessageContent, MessageId, Qos) of
        {error, Code, Msg} ->
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
        ok ->
            case Qos of
                0 ->
                    TopicTemplate = resolve_topic(TopicTemplateName, TopicShortName, ProductKey),
                    case resolve_qos0_payload(MessageContent, MessageId) of
                        {ok, Payload, ApiMsgId} ->
                            emqx_bcast_metrics:qos0_in(),
                            emqx_bcast_metrics:qos0_targeted(length(DeviceNames)),
                            deliver_qos0(
                                DeviceNames, ProductKey, TopicTemplate, Payload, RequestId, ApiMsgId
                            );
                        {error, Code, Msg} ->
                            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)}
                    end;
                1 ->
                    case validate(ProductKey, DeviceNames, MessageContent, MessageId) of
                        {error, Code, Msg} ->
                            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
                        {ok, ApiMsgId, MsgGuid} ->
                            TopicTemplate = resolve_topic(
                                TopicTemplateName, TopicShortName, ProductKey
                            ),
                            deliver_qos1(
                                DeviceNames,
                                ProductKey,
                                TopicTemplate,
                                MsgGuid,
                                RequestId,
                                ApiMsgId
                            )
                    end
            end
    end.

resolve_qos0_payload(MessageContent, _MessageId) when MessageContent =/= undefined ->
    {ok, Payload} = emqx_bcast_utils:decode_base64(MessageContent),
    MaxSize = get_max_message_size_batch(),
    case byte_size(Payload) =< MaxSize of
        true -> {ok, Payload, emqx_bcast_utils:gen_api_uuid()};
        false -> {error, <<"MessageTooLarge">>, <<"Message too large">>}
    end;
resolve_qos0_payload(undefined, MessageId) ->
    case emqx_bcast_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
            {ok, Msg#bcast_message.payload, MessageId};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

validate_input(_PK, undefined, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName is required">>};
validate_input(_PK, _DeviceNames, undefined, undefined, _Qos) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate_input(_PK, _DeviceNames, _MC, _MI, _Qos) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate_input(_PK, _DeviceNames, _MC, _MI, Qos) when Qos =/= 0, Qos =/= 1 ->
    {error, <<"InvalidQos">>, <<"QoS must be 0 or 1">>};
validate_input(_PK, DeviceNames, _MC, _MI, _Qos) when is_list(DeviceNames) ->
    Max = get_max_device_count(),
    case length(DeviceNames) > Max of
        true ->
            {error, <<"DeviceCountExceeded">>, <<"Too many devices">>};
        false ->
            case has_duplicates(DeviceNames) of
                true -> {error, <<"DuplicateDeviceName">>, <<"Duplicate DeviceName entries">>};
                false -> ok
            end
    end;
validate_input(_, _, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName must be a list">>}.

validate(_PK, undefined, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName is required">>};
validate(_PK, _DeviceNames, undefined, undefined) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate(_PK, _DeviceNames, _MC, _MI) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate(_PK, DeviceNames, _MC, _MI) when is_list(DeviceNames) ->
    Max = get_max_device_count(),
    case length(DeviceNames) > Max of
        true ->
            {error, <<"DeviceCountExceeded">>, <<"Too many devices">>};
        false ->
            case has_duplicates(DeviceNames) of
                true -> {error, <<"DuplicateDeviceName">>, <<"Duplicate DeviceName entries">>};
                false -> resolve_content(DeviceNames, _MC, _MI)
            end
    end.

resolve_content(_DeviceNames, MessageContent, _MessageId) when MessageContent =/= undefined ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            Hash = emqx_bcast_utils:sha256(Payload),
            {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
            case emqx_bcast_storage:lookup_or_create_message(Payload, Hash, ApiMsgId, MsgGuid) of
                {created, Id, Guid} -> {ok, Id, Guid};
                {existing, Id, Guid} -> {ok, Id, Guid};
                {error, _} -> {error, <<"InternalError">>, <<"Storage error">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
resolve_content(_DeviceNames, _MC, MessageId) when MessageId =/= undefined ->
    case emqx_bcast_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            _ = emqx_bcast_storage:refresh_message_ttl(MsgGuid),
            {ok, MessageId, MsgGuid};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

deliver_qos0(DeviceNames, ProductKey, TopicTemplate, Payload, RequestId, ApiMsgId) ->
    lists:foreach(
        fun(DN) ->
            case emqx_bcast:lookup_device({ProductKey, DN}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DN),
                    case emqx_bcast_subscription:match(DN, Topic) of
                        {ok, _SubQos} ->
                            emqx_bcast_metrics:qos0_delivered(),
                            Msg = emqx_message:make(DN, ?QOS_0, Topic, Payload),
                            Pid ! #deliver{topic = Topic, message = Msg};
                        false ->
                            emqx_bcast_metrics:qos0_skipped()
                    end;
                _ ->
                    emqx_bcast_metrics:qos0_skipped()
            end
        end,
        DeviceNames
    ),
    {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)}.

deliver_qos1(DeviceNames, ProductKey, TopicTemplate, MsgGuid, RequestId, ApiMsgId) ->
    emqx_bcast_metrics:qos1_in(),
    emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    N = length(DeviceNames),
    _Delivery = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, ProductKey, TopicTemplate, DeviceNames, N
    ),
    {ok, PayloadMsg} = emqx_bcast_storage:lookup_message(MsgGuid),
    Payload = PayloadMsg#bcast_message.payload,
    Config = persistent_term:get({?APP, config}, #{}),
    ForceUpgrade = maps:get(force_upgrade_qos, Config, true),
    lists:foreach(
        fun(DN) ->
            case emqx_bcast:lookup_device({ProductKey, DN}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DN),
                    case emqx_bcast_subscription:match(DN, Topic) of
                        {ok, SubQos} ->
                            case ForceUpgrade orelse SubQos >= 1 of
                                true ->
                                    emqx_bcast_metrics:qos1_delivered_inline(),
                                    Msg = emqx_message:make(
                                        DeliveryId,
                                        DN,
                                        ?QOS_1,
                                        Topic,
                                        Payload,
                                        #{},
                                        #{?IOT_DELIVERY_ID => DeliveryId}
                                    ),
                                    Pid ! #deliver{topic = Topic, message = Msg};
                                false ->
                                    Msg = emqx_message:make(DN, ?QOS_0, Topic, Payload),
                                    Pid ! #deliver{topic = Topic, message = Msg},
                                    _ = emqx_bcast_storage:process_ack(
                                        ProductKey, DN, DeliveryId
                                    ),
                                    emqx_bcast_metrics:qos1_acked()
                            end;
                        false ->
                            emqx_bcast_metrics:qos1_stored_offline()
                    end;
                _ ->
                    emqx_bcast_metrics:qos1_stored_offline()
            end
        end,
        DeviceNames
    ),
    {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)}.

has_duplicates(List) ->
    length(lists:usort(List)) =/= length(List).

resolve_topic(TemplateName, _, _Pk) when TemplateName =/= undefined ->
    TemplateName;
resolve_topic(_, ShortName, Pk) when ShortName =/= undefined ->
    <<"/", Pk/binary, "/${deviceName}/user/", ShortName/binary>>;
resolve_topic(_, _, Pk) ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(batch_topic, Config, <<"/", Pk/binary, "/${deviceName}/user/get">>).

get_max_device_count() ->
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    maps:get(max_device_count, Config, 10000).

get_max_message_size_batch() ->
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    maps:get(max_message_size_batch, Config, 10240).
