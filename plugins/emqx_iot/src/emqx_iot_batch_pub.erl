%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_batch_pub).

-export([handle/2]).

-include("emqx_iot.hrl").

handle(Body, RequestId) ->
    ProductKey = maps:get(<<"ProductKey">>, Body, undefined),
    DeviceNames = maps:get(<<"DeviceName">>, Body, undefined),
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    MessageId = maps:get(<<"MessageId">>, Body, undefined),
    Qos = maps:get(<<"Qos">>, Body, 0),
    TopicShortName = maps:get(<<"TopicShortName">>, Body, undefined),
    TopicTemplateName = maps:get(<<"TopicTemplateName">>, Body, undefined),
    ResponseTemplate = maps:get(<<"ResponseTopicTemplateName">>, Body, undefined),

    case validate(ProductKey, DeviceNames, MessageContent, MessageId) of
        {error, Code, Msg} ->
            {ok, 400, #{}, emqx_iot_api:error_response(RequestId, Code, Msg)};
        {ok, ApiMsgId, MsgGuid} ->
            TopicTemplate = resolve_topic(TopicTemplateName, TopicShortName, ProductKey),
            case Qos of
                0 ->
                    emqx_iot_metrics:inc_batch_pub_qos0_in(),
                    emqx_iot_metrics:inc_qos0_targeted(length(DeviceNames)),
                    deliver_qos0(
                        DeviceNames, ProductKey, TopicTemplate, MsgGuid, RequestId, ApiMsgId
                    );
                1 ->
                    deliver_qos1(
                        DeviceNames,
                        ProductKey,
                        TopicTemplate,
                        MsgGuid,
                        RequestId,
                        ApiMsgId,
                        ResponseTemplate
                    )
            end
    end.

validate(_PK, undefined, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName is required">>};
validate(_PK, _DeviceNames, undefined, undefined) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate(_PK, _DeviceNames, _MC, _MI) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate(_PK, DeviceNames, _MC, _MI) when is_list(DeviceNames), length(DeviceNames) > 10000 ->
    {error, <<"DeviceCountExceeded">>, <<"Too many devices">>};
validate(_PK, DeviceNames, _MC, _MI) when is_list(DeviceNames) ->
    case has_duplicates(DeviceNames) of
        true -> {error, <<"DuplicateDeviceName">>, <<"Duplicate DeviceName entries">>};
        false -> resolve_content(DeviceNames, _MC, _MI)
    end.

resolve_content(_DeviceNames, MessageContent, _MessageId) when MessageContent =/= undefined ->
    case emqx_iot_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            Hash = emqx_iot_utils:sha256(Payload),
            case emqx_iot_storage:lookup_message_by_hash(Hash) of
                {ok, Existing} ->
                    {ok, Existing#iot_mq_message.api_msg_id, Existing#iot_mq_message.msg_id};
                {error, not_found} ->
                    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
                    ok = emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
                    {ok, ApiMsgId, MsgGuid}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
resolve_content(_DeviceNames, _MC, MessageId) when MessageId =/= undefined ->
    case emqx_iot_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            _ = emqx_iot_storage:refresh_message_ttl(MsgGuid),
            {ok, MessageId, MsgGuid};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

deliver_qos0(DeviceNames, ProductKey, TopicTemplate, MsgGuid, RequestId, ApiMsgId) ->
    {ok, PayloadMsg} = emqx_iot_storage:lookup_message(MsgGuid),
    Payload = PayloadMsg#iot_mq_message.payload,
    lists:foreach(
        fun(DN) ->
            case emqx_iot:lookup_device({ProductKey, DN}) of
                {ok, Pid} ->
                    emqx_iot_metrics:inc_qos0_delivered(),
                    Topic = emqx_iot_utils:expand_topic(TopicTemplate, ProductKey, DN),
                    Msg = emqx_message:make(DN, ?QOS_0, Topic, Payload),
                    Pid ! #deliver{topic = Topic, message = Msg};
                _ ->
                    emqx_iot_metrics:inc_qos0_skipped()
            end
        end,
        DeviceNames
    ),
    {ok, 200, #{}, emqx_iot_api:success_response(RequestId, ApiMsgId)}.

deliver_qos1(
    DeviceNames, ProductKey, TopicTemplate, MsgGuid, RequestId, ApiMsgId, ResponseTemplate
) ->
    emqx_iot_metrics:inc_batch_pub_qos1_in(),
    emqx_iot_metrics:inc_msg_wanted(length(DeviceNames)),
    DeliveryId = emqx_iot_utils:gen_guid(),
    N = length(DeviceNames),
    _Delivery = emqx_iot_storage:create_delivery(
        DeliveryId, MsgGuid, ProductKey, TopicTemplate, DeviceNames, N, ResponseTemplate
    ),
    {ok, PayloadMsg} = emqx_iot_storage:lookup_message(MsgGuid),
    Payload = PayloadMsg#iot_mq_message.payload,
    lists:foreach(
        fun(DN) ->
            case emqx_iot:lookup_device({ProductKey, DN}) of
                {ok, Pid} ->
                    emqx_iot_metrics:inc_qos1_delivered_inline(),
                    Topic = emqx_iot_utils:expand_topic(TopicTemplate, ProductKey, DN),
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
                _ ->
                    emqx_iot_metrics:inc_qos1_stored_offline()
            end
        end,
        DeviceNames
    ),
    {ok, 200, #{}, emqx_iot_api:success_response(RequestId, ApiMsgId)}.

resolve_topic(TopicTemplateName, _TopicShortName, _ProductKey) when
    TopicTemplateName =/= undefined
->
    TopicTemplateName;
resolve_topic(undefined, TopicShortName, ProductKey) when TopicShortName =/= undefined ->
    <<"/", ProductKey/binary, "/${deviceName}/user/", TopicShortName/binary>>;
resolve_topic(undefined, undefined, _ProductKey) ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(batch_topic, Config, <<"/${productKey}/${deviceName}/user/get">>).

has_duplicates(List) ->
    length(List) =/= sets:size(sets:from_list(List)).
