%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_register_message).

-export([handle/2]).

-include("emqx_iot.hrl").

handle(Body, RequestId) ->
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    MessageId = maps:get(<<"MessageId">>, Body, undefined),

    case validate(MessageContent, MessageId) of
        {error, Code, Msg} ->
            emqx_iot_metrics:inc_register_message_error(),
            {ok, 400, #{}, emqx_iot_api:error_response(RequestId, Code, Msg)};
        {create, Payload} ->
            do_create(Payload, RequestId);
        {refresh, ApiMsgId} ->
            do_refresh(ApiMsgId, RequestId)
    end.

validate(undefined, undefined) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate(_MC, _MI) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate(MessageContent, _MI) when MessageContent =/= undefined ->
    case emqx_iot_utils:decode_base64(MessageContent) of
        {ok, Payload} -> {create, Payload};
        {error, _} -> {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
validate(_MC, MessageId) when MessageId =/= undefined ->
    {refresh, MessageId}.

do_create(Payload, RequestId) ->
    Hash = emqx_iot_utils:sha256(Payload),
    case emqx_iot_storage:lookup_message_by_hash(Hash) of
        {ok, Existing} ->
            _ = emqx_iot_storage:refresh_message_ttl(Existing#iot_mq_message.msg_id),
            emqx_iot_metrics:inc_register_message_refresh(),
            {ok, 200, #{},
                emqx_iot_api:success_response(RequestId, Existing#iot_mq_message.api_msg_id)};
        {error, not_found} ->
            {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
            ok = emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
            emqx_iot_metrics:inc_register_message_in(),
            {ok, 200, #{}, emqx_iot_api:success_response(RequestId, ApiMsgId)}
    end.

do_refresh(ApiMsgId, RequestId) ->
    case emqx_iot_id:resolve_message_id(ApiMsgId) of
        {ok, MsgGuid} ->
            _ = emqx_iot_storage:refresh_message_ttl(MsgGuid),
            emqx_iot_metrics:inc_register_message_refresh(),
            {ok, 200, #{}, emqx_iot_api:success_response(RequestId, ApiMsgId)};
        {error, not_found} ->
            emqx_iot_metrics:inc_register_message_error(),
            {ok, 400, #{},
                emqx_iot_api:error_response(
                    RequestId, <<"MessageNotFound">>, <<"MessageId not found">>
                )}
    end.
