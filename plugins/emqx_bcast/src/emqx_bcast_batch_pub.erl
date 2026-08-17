%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_batch_pub).

-export([handle/2]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(WORKER_POOL, emqx_bcast_pull_server_worker_pool).

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
            TopicTemplate = resolve_topic(TopicTemplateName, TopicShortName, ProductKey),
            case Qos of
                0 ->
                    do_qos0(
                        DeviceNames, ProductKey, TopicTemplate, MessageContent, MessageId, RequestId
                    );
                1 ->
                    do_qos1(
                        DeviceNames, ProductKey, TopicTemplate, MessageContent, MessageId, RequestId
                    )
            end
    end.

%%--------------------------------------------------------------------
%% QoS0: one-shot delivery, no storage, no pending, no retry.
%% Core broadcasts full deliver data to every pull_pool; each pull_pool
%% checks online + subscription and drops otherwise.
%%--------------------------------------------------------------------

do_qos0(DeviceNames, ProductKey, TopicTemplate, MessageContent, MessageId, RequestId) ->
    case resolve_qos0_payload(MessageContent, MessageId) of
        {ok, Payload, ApiMsgId} ->
            emqx_bcast_metrics:qos0_in(),
            emqx_bcast_metrics:qos0_targeted(length(DeviceNames)),
            ok = emqx_bcast_pull_server_pool:qos0_broadcast(
                ProductKey, TopicTemplate, Payload
            ),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)};
        {error, Code, Msg} ->
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)}
    end.

resolve_qos0_payload(MessageContent, _MessageId) when MessageContent =/= undefined ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            MaxSize = get_max_message_size_batch(),
            case byte_size(Payload) =< MaxSize of
                true -> {ok, Payload, emqx_bcast_utils:gen_api_uuid()};
                false -> {error, <<"MessageTooLarge">>, <<"Message too large">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
resolve_qos0_payload(undefined, MessageId) ->
    case emqx_bcast_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
            {ok, Msg#bcast_message.payload, MessageId};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

%%--------------------------------------------------------------------
%% QoS1: authoritative storage on core, then broadcast a pure trigger
%% (no payload). Pull pools turn it into want_next batches.
%%--------------------------------------------------------------------

do_qos1(DeviceNames, ProductKey, TopicTemplate, MessageContent, MessageId, RequestId) ->
    case prepare_qos1_content(MessageContent, MessageId) of
        {error, Code, Msg} ->
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
        {ok, {content, Payload, Hash}} ->
            {ApiMsgId, MsgGuid} = resolve_content_ids(Hash),
            DeliveryId = emqx_bcast_utils:gen_guid(),
            emqx_bcast_metrics:qos1_in(),
            emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
            ok = submit_qos1_task(fun() ->
                persist_content_and_trigger(
                    Payload,
                    Hash,
                    ApiMsgId,
                    MsgGuid,
                    DeliveryId,
                    ProductKey,
                    TopicTemplate,
                    DeviceNames
                )
            end),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)};
        {ok, {reuse, ApiMsgId, MsgGuid}} ->
            DeliveryId = emqx_bcast_utils:gen_guid(),
            emqx_bcast_metrics:qos1_in(),
            emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
            ok = submit_qos1_task(fun() ->
                persist_reuse_and_trigger(
                    DeliveryId,
                    MsgGuid,
                    ProductKey,
                    TopicTemplate,
                    DeviceNames
                )
            end),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)}
    end.

resolve_content_ids(Hash) ->
    case emqx_bcast_storage:lookup_message_by_hash(Hash) of
        {ok, #bcast_message{api_msg_id = ExistingApiMsgId, msg_id = ExistingMsgId}} ->
            {ExistingApiMsgId, ExistingMsgId};
        {error, not_found} ->
            emqx_bcast_id:generate_message_id_from_hash(Hash)
    end.

submit_qos1_task(Fun) ->
    try
        emqx_pool:async_submit_to_pool(?WORKER_POOL, Fun)
    catch
        _:_ -> Fun()
    end.

persist_content_and_trigger(
    Payload, Hash, ApiMsgId, MsgGuid, DeliveryId, ProductKey, TopicTemplate, DeviceNames
) ->
    case
        emqx_bcast_storage:create_message_and_delivery(
            Payload,
            Hash,
            ApiMsgId,
            MsgGuid,
            DeliveryId,
            ProductKey,
            TopicTemplate,
            DeviceNames
        )
    of
        {ok, _ResolvedApiMsgId, _Delivery} ->
            ok = emqx_bcast_pull_server_pool:qos1_trigger(
                ProductKey, DeviceNames, TopicTemplate
            );
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "bcast_qos1_persist_failed",
                api_msg_id => ApiMsgId,
                delivery_id => DeliveryId,
                reason => Reason
            })
    end,
    ok.

persist_reuse_and_trigger(DeliveryId, MsgGuid, ProductKey, TopicTemplate, DeviceNames) ->
    _Delivery = emqx_bcast_storage:create_delivery(
        DeliveryId,
        MsgGuid,
        ProductKey,
        TopicTemplate,
        DeviceNames,
        length(DeviceNames)
    ),
    _ = emqx_bcast_storage:refresh_message_ttl(MsgGuid),
    ok = emqx_bcast_pull_server_pool:qos1_trigger(ProductKey, DeviceNames, TopicTemplate),
    ok.

prepare_qos1_content(MessageContent, _MessageId) when MessageContent =/= undefined ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            MaxSize = get_max_message_size_batch(),
            case byte_size(Payload) =< MaxSize of
                true ->
                    Hash = emqx_bcast_utils:sha256(Payload),
                    {ok, {content, Payload, Hash}};
                false ->
                    {error, <<"MessageTooLarge">>, <<"Message too large">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
prepare_qos1_content(undefined, MessageId) ->
    case emqx_bcast_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            {ok, {reuse, MessageId, MsgGuid}};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

%%--------------------------------------------------------------------
%% Validation
%%--------------------------------------------------------------------

validate_input(PK, _, _, _, _) when not is_binary(PK) orelse PK =:= <<>> ->
    {error, <<"InvalidProductKey">>, <<"ProductKey is required">>};
validate_input(_PK, undefined, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName is required">>};
validate_input(_PK, _DeviceNames, undefined, undefined, _Qos) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate_input(_PK, _DeviceNames, _MC, _MI, _Qos) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate_input(_PK, _DeviceNames, _MC, _MI, Qos) when Qos =/= 0, Qos =/= 1 ->
    {error, <<"InvalidQos">>, <<"QoS must be 0 or 1">>};
validate_input(_PK, DeviceNames, _MC, _MI, _Qos) when is_list(DeviceNames) ->
    case lists:all(fun erlang:is_binary/1, DeviceNames) of
        false ->
            {error, <<"InvalidDeviceName">>, <<"DeviceName entries must be strings">>};
        true ->
            Max = get_max_device_count(),
            case length(DeviceNames) > Max of
                true ->
                    {error, <<"DeviceCountExceeded">>, <<"Too many devices">>};
                false ->
                    case has_duplicates(DeviceNames) of
                        true ->
                            {error, <<"DuplicateDeviceName">>, <<"Duplicate DeviceName entries">>};
                        false ->
                            ok
                    end
            end
    end;
validate_input(_, _, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName must be a list">>}.

resolve_topic(TemplateName, _, _Pk) when TemplateName =/= undefined ->
    TemplateName;
resolve_topic(_, ShortName, Pk) when ShortName =/= undefined ->
    <<"/", Pk/binary, "/${deviceName}/user/", ShortName/binary>>;
resolve_topic(_, _, Pk) ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(batch_topic, Config, <<"/", Pk/binary, "/${deviceName}/user/get">>).

has_duplicates(List) ->
    length(lists:usort(List)) =/= length(List).

get_max_device_count() ->
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    maps:get(max_device_count, Config, 10000).

get_max_message_size_batch() ->
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    maps:get(max_message_size_batch, Config, 10240).
