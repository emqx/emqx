%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_batch_pub).

-export([handle/2]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

handle(Body, RequestId) ->
    ProductKey = maps:get(<<"ProductKey">>, Body, undefined),
    DeviceNames = maps:get(<<"DeviceName">>, Body, undefined),
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    MessageId = maps:get(<<"MessageId">>, Body, undefined),
    Qos = maps:get(<<"Qos">>, Body, 0),
    TopicShortName = maps:get(<<"TopicShortName">>, Body, undefined),
    TopicTemplateName = maps:get(<<"TopicTemplateName">>, Body, undefined),

    case
        validate_input(
            ProductKey,
            DeviceNames,
            MessageContent,
            MessageId,
            Qos,
            TopicShortName,
            TopicTemplateName
        )
    of
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
                ProductKey, DeviceNames, TopicTemplate, Payload
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
            case emqx_bcast_storage:lookup_message(MsgGuid) of
                {ok, Msg} ->
                    {ok, Msg#bcast_message.payload, MessageId};
                {error, not_found} ->
                    {error, <<"MessageNotFound">>, <<"MessageId not found">>}
            end;
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
        {ok, Content} ->
            do_qos1_intake(Content, DeviceNames, ProductKey, TopicTemplate, RequestId)
    end.

%% Async acceptance: the request is enqueued into the node-local intake
%% queue and the HTTP 200 is returned immediately (SLO: in-memory
%% acceptance; the subscriber's PUBACK is the final confirmation). The
%% promoter then commits it to mria (delivery + message rows, the
%% durability point) and appends the per-device index on the owner core.
%% Entries still queued when a node dies may be lost (by contract);
%% everything committed to mria survives single-node crashes.
do_qos1_intake({content, Payload, Hash}, DeviceNames, ProductKey, TopicTemplate, RequestId) ->
    {ApiMsgId, MsgGuid} = resolve_content_ids(Hash),
    enqueue_request(
        Payload, Hash, ApiMsgId, MsgGuid, DeviceNames, ProductKey, TopicTemplate, RequestId
    );
do_qos1_intake({reuse, ApiMsgId, MsgGuid}, DeviceNames, ProductKey, TopicTemplate, RequestId) ->
    case emqx_bcast_storage:lookup_message(MsgGuid) of
        {ok, #bcast_message{payload = Payload, content_hash = Hash}} ->
            enqueue_request(
                Payload, Hash, ApiMsgId, MsgGuid, DeviceNames, ProductKey, TopicTemplate, RequestId
            );
        {error, not_found} ->
            {ok, 400, #{},
                emqx_bcast_api:error_response(
                    RequestId, <<"MessageNotFound">>, <<"MessageId not found">>
                )}
    end.

enqueue_request(
    Payload, Hash, ApiMsgId, MsgGuid, DeviceNames, ProductKey, TopicTemplate, RequestId
) ->
    emqx_bcast_metrics:qos1_in(),
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    Entry = #{
        payload => Payload,
        hash => Hash,
        api_msg_id => ApiMsgId,
        msg_id => MsgGuid,
        delivery_id => emqx_bcast_utils:gen_guid(),
        product_key => ProductKey,
        topic_template => TopicTemplate,
        devices => DeviceNames,
        created_at => Now,
        expires_at => Now + TTL
    },
    %% Reject on a full queue BEFORE the admission work: a backpressured
    %% loader otherwise burns a full 1000-device admit (+release) per
    %% rejected request, saturating the index shards the promoters need
    %% for appends (measured: shard 0 backed up at 1600 rejected req/s).
    %% Arity-2 get: stale config maps (e.g. older suites) may lack the key.
    MaxDepth = emqx_bcast_config:get(intake_queue_depth, 20000),
    case emqx_bcast_intake:depth() >= MaxDepth of
        true ->
            emqx_bcast_metrics:intake_rejected(),
            {ok, 429, #{},
                emqx_bcast_api:error_response(
                    RequestId, <<"Busy">>, <<"Intake queue full, retry later">>, #{}
                )};
        false ->
            %% Atomic admission: the owner checks the pending quota and
            %% reserves one slot per device in the same serialized step, so
            %% concurrent requests cannot all pass a stale count. The
            %% reservation converts into a real index entry when the
            %% promoter appends, and is released here if the queue rejects
            %% the entry (racy tail). During an owner takeover the call
            %% degrades to `ok` (admission pressure deferred to the bounded
            %% queue) instead of erroring the request.
            case quota_admit(ProductKey, DeviceNames) of
                ok ->
                    case emqx_bcast_intake:enqueue(Entry) of
                        {ok, _Seq} ->
                            emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
                            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)};
                        full ->
                            _ = emqx_bcast_index_owner:release_admit(ProductKey, DeviceNames),
                            {ok, 429, #{},
                                emqx_bcast_api:error_response(
                                    RequestId, <<"Busy">>, <<"Intake queue full, retry later">>, #{}
                                )}
                    end;
                {error, {quota_exceeded, OverLimit}} ->
                    quota_exceeded_response(RequestId, OverLimit)
            end
    end.

quota_admit(ProductKey, DeviceNames) ->
    try emqx_bcast_index_owner:admit(ProductKey, DeviceNames) of
        ok ->
            ok;
        {error, {quota_exceeded, _} = Exceeded} ->
            {error, Exceeded};
        _Other ->
            %% Owner not active yet (startup/takeover): degrade to
            %% acceptance; the bounded queue provides the backpressure.
            ok
    catch
        _:_ ->
            ok
    end.

resolve_content_ids(Hash) ->
    case emqx_bcast_storage:lookup_message_by_hash(Hash) of
        {ok, #bcast_message{api_msg_id = ExistingApiMsgId, msg_id = ExistingMsgId}} ->
            {ExistingApiMsgId, ExistingMsgId};
        {error, not_found} ->
            emqx_bcast_id:generate_message_id_from_hash(Hash)
    end.

quota_exceeded_response(RequestId, []) ->
    {ok, 429, #{},
        emqx_bcast_api:error_response(
            RequestId, <<"QuotaExceeded">>, <<"Pending delivery quota exceeded">>, #{}
        )};
quota_exceeded_response(RequestId, OverLimit) ->
    {ok, 429, #{},
        emqx_bcast_api:error_response(
            RequestId,
            <<"QuotaExceeded">>,
            <<"Device pending delivery quota exceeded">>,
            #{<<"Devices">> => OverLimit}
        )}.

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

validate_input(PK, DNs, MC, MI, Qos, ShortName, TemplateName) ->
    case validate_product_key(PK) of
        ok -> validate_input2(DNs, MC, MI, Qos, ShortName, TemplateName);
        Error -> Error
    end.

validate_product_key(PK) when not is_binary(PK) orelse PK =:= <<>> ->
    {error, <<"InvalidProductKey">>, <<"ProductKey is required">>};
validate_product_key(PK) ->
    case contains_any(PK, [<<"/">>, <<"+">>, <<"#">>, <<"$">>]) of
        false -> ok;
        true -> {error, <<"InvalidProductKey">>, <<"ProductKey contains invalid characters">>}
    end.

validate_input2(undefined, _, _, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName is required">>};
validate_input2(_DeviceNames, undefined, undefined, _Qos, _, _) ->
    {error, <<"MessageIdContentConflict">>, <<"MessageContent or MessageId required">>};
validate_input2(_DeviceNames, _MC, _MI, _Qos, _, _) when _MC =/= undefined, _MI =/= undefined ->
    {error, <<"MessageIdContentConflict">>, <<"Only one of MessageContent or MessageId allowed">>};
validate_input2(_DeviceNames, _MC, _MI, Qos, _, _) when Qos =/= 0, Qos =/= 1 ->
    {error, <<"InvalidQos">>, <<"QoS must be 0 or 1">>};
validate_input2(DeviceNames, _MC, _MI, _Qos, ShortName, TemplateName) when is_list(DeviceNames) ->
    case validate_topic_fields(ShortName, TemplateName) of
        ok ->
            case validate_device_names(DeviceNames) of
                ok ->
                    Max = get_max_device_count(),
                    case length(DeviceNames) > Max of
                        true ->
                            {error, <<"DeviceCountExceeded">>, <<"Too many devices">>};
                        false ->
                            case has_duplicates(DeviceNames) of
                                true ->
                                    {error, <<"DuplicateDeviceName">>,
                                        <<"Duplicate DeviceName entries">>};
                                false ->
                                    ok
                            end
                    end;
                Error ->
                    Error
            end;
        {error, Code, Msg} ->
            {error, Code, Msg}
    end;
validate_input2(_, _, _, _, _, _) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName must be a list">>}.

%% DeviceName becomes a topic level, so reject wildcards and separators in
%% addition to the non-binary check. An empty list would create a delivery
%% with target_ack_count = 0 that never completes, so reject it too.
validate_device_names([]) ->
    {error, <<"InvalidDeviceName">>, <<"DeviceName must not be empty">>};
validate_device_names(DeviceNames) ->
    case lists:all(fun erlang:is_binary/1, DeviceNames) of
        false ->
            {error, <<"InvalidDeviceName">>, <<"DeviceName entries must be strings">>};
        true ->
            case
                lists:all(
                    fun(DN) ->
                        case contains_any(DN, [<<"/">>, <<"+">>, <<"#">>, <<"$">>]) of
                            false -> true;
                            true -> false
                        end
                    end,
                    DeviceNames
                )
            of
                true ->
                    ok;
                false ->
                    {error, <<"InvalidDeviceName">>, <<"DeviceName contains invalid characters">>}
            end
    end.

%% TopicShortName is a suffix appended to the delivery topic, so it must not
%% carry topic separators, wildcards or placeholder syntax. TopicTemplateName
%% is a full template: reject wildcards (they would turn the delivery topic
%% into a subscription filter) and any placeholder other than ${productKey}
%% and ${deviceName}, which expand_topic/3 resolves before delivery.
validate_topic_fields(ShortName, TemplateName) ->
    case validate_short_name(ShortName) of
        ok -> validate_template_name(TemplateName);
        Error -> Error
    end.

validate_short_name(undefined) ->
    ok;
validate_short_name(ShortName) when is_binary(ShortName) ->
    case contains_any(ShortName, [<<"/">>, <<"+">>, <<"#">>, <<"$">>, <<"{">>, <<"}">>]) of
        false ->
            ok;
        true ->
            {error, <<"InvalidTopicTemplate">>, <<"TopicShortName contains invalid characters">>}
    end;
validate_short_name(_) ->
    {error, <<"InvalidTopicTemplate">>, <<"TopicShortName must be a string">>}.

validate_template_name(undefined) ->
    ok;
validate_template_name(TemplateName) when is_binary(TemplateName) ->
    case contains_any(TemplateName, [<<"+">>, <<"#">>]) of
        false -> validate_placeholders(TemplateName);
        true -> {error, <<"InvalidTopicTemplate">>, <<"TopicTemplateName contains wildcards">>}
    end;
validate_template_name(_) ->
    {error, <<"InvalidTopicTemplate">>, <<"TopicTemplateName must be a string">>}.

%% Byte scan for any of the constant ASCII patterns. Faster than a
%% per-call re:run (which recompiles the pattern every time) and the
%% rejected characters are all single-byte ASCII. Returns true when any
%% pattern occurs; callers must test the BOOLEAN (an earlier version kept
%% kept the old re:run-style nomatch case branches, which rejected every
%% valid product key with InvalidProductKey and broke all BatchPub /
%% RegisterMessage API calls).
contains_any(Bin, Patterns) ->
    binary:match(Bin, Patterns) =/= nomatch.

validate_placeholders(TemplateName) ->
    %% Remove the supported placeholders, then reject any remaining ${...}
    %% syntax (unknown placeholders would silently leak into the topic).
    Rest0 = binary:replace(TemplateName, <<"${productKey}">>, <<>>, [global]),
    Rest = binary:replace(Rest0, <<"${deviceName}">>, <<>>, [global]),
    case binary:match(Rest, <<"${">>) of
        nomatch -> ok;
        _ -> {error, <<"InvalidTopicTemplate">>, <<"TopicTemplateName has invalid placeholders">>}
    end.

resolve_topic(TemplateName, _, _Pk) when TemplateName =/= undefined ->
    TemplateName;
resolve_topic(_, ShortName, Pk) when ShortName =/= undefined ->
    <<"/", Pk/binary, "/${deviceName}/user/", ShortName/binary>>;
resolve_topic(_, _, _Pk) ->
    emqx_bcast_config:get(batch_topic).

has_duplicates(List) ->
    length(lists:usort(List)) =/= length(List).

get_max_device_count() ->
    emqx_bcast_config:get(max_device_count).

get_max_message_size_batch() ->
    emqx_bcast_config:get(max_message_size_batch).
