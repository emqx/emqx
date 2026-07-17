%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_register_message).

-export([handle/2]).

-include("emqx_bcast.hrl").

handle(Body, RequestId) ->
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    MessageId = maps:get(<<"MessageId">>, Body, undefined),

    case validate(MessageContent, MessageId) of
        {error, Code, Msg} ->
            emqx_bcast_metrics:register_error(),
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
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
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} -> {create, Payload};
        {error, _} -> {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
validate(_MC, MessageId) when MessageId =/= undefined ->
    {refresh, MessageId}.

do_create(Payload, RequestId) ->
    Hash = emqx_bcast_utils:sha256(Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    case emqx_bcast_storage:lookup_or_create_message(Payload, Hash, ApiMsgId, MsgGuid) of
        {existing, Id, _} ->
            emqx_bcast_metrics:register_refresh(),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, Id)};
        {created, Id, _} ->
            emqx_bcast_metrics:register_in(),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, Id)};
        {error, _} ->
            emqx_bcast_metrics:register_error(),
            {ok, 500, #{},
                emqx_bcast_api:error_response(RequestId, <<"InternalError">>, <<"Storage error">>)}
    end.

do_refresh(ApiMsgId, RequestId) ->
    case emqx_bcast_id:resolve_message_id(ApiMsgId) of
        {ok, MsgGuid} ->
            _ = emqx_bcast_storage:refresh_message_ttl(MsgGuid),
            emqx_bcast_metrics:register_refresh(),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)};
        {error, not_found} ->
            emqx_bcast_metrics:register_error(),
            {ok, 400, #{},
                emqx_bcast_api:error_response(
                    RequestId, <<"MessageNotFound">>, <<"MessageId not found">>
                )}
    end.
