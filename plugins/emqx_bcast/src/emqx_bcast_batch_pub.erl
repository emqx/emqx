%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_batch_pub).

-export([handle/2, resolve_local/3]).

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
                        {ok, Prepared} ->
                            TopicTemplate = resolve_topic(
                                TopicTemplateName, TopicShortName, ProductKey
                            ),
                            deliver_qos1(
                                Prepared,
                                DeviceNames,
                                ProductKey,
                                TopicTemplate,
                                RequestId
                            )
                    end
            end
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
                true ->
                    {error, <<"DuplicateDeviceName">>, <<"Duplicate DeviceName entries">>};
                false ->
                    prepare_content(_MC, _MI)
            end
    end.

%% Prepare the message reference without touching storage. Storage writes
%% happen only after the queue capacity check passes, so a 429 response
%% leaves no records behind.
prepare_content(MessageContent, _MessageId) when MessageContent =/= undefined ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            MaxSize = get_max_message_size_batch(),
            case byte_size(Payload) =< MaxSize of
                true ->
                    Hash = emqx_bcast_utils:sha256(Payload),
                    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
                    {ok, {content, Payload, Hash, ApiMsgId, MsgGuid}};
                false ->
                    {error, <<"MessageTooLarge">>, <<"Message too large">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end;
prepare_content(_MC, MessageId) when MessageId =/= undefined ->
    case emqx_bcast_id:resolve_message_id(MessageId) of
        {ok, MsgGuid} ->
            {ok, {reuse, MessageId, MsgGuid}};
        {error, not_found} ->
            {error, <<"MessageNotFound">>, <<"MessageId not found">>}
    end.

deliver_qos0(DeviceNames, ProductKey, TopicTemplate, Payload, RequestId, ApiMsgId) ->
    {Targets, Offline} = resolve(DeviceNames, ProductKey, TopicTemplate),
    ok = count_offline(0, Offline),
    case emqx_bcast_deliver:has_capacity(length(Targets)) of
        false ->
            queue_full_response(RequestId);
        true ->
            _ = emqx_bcast_deliver:submit_targets(Targets, #{qos => 0, payload => Payload}),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)}
    end.

deliver_qos1(
    {content, Payload, Hash, ApiMsgId0, MsgGuid}, DeviceNames, ProductKey, TopicTemplate, RequestId
) ->
    emqx_bcast_metrics:qos1_in(),
    emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
    {Targets, Offline} = resolve(DeviceNames, ProductKey, TopicTemplate),
    case emqx_bcast_deliver:try_reserve(length(Targets)) of
        {error, overloaded} ->
            queue_full_response(RequestId);
        ok ->
            DeliveryId = emqx_bcast_utils:gen_guid(),
            case
                emqx_bcast_storage:create_message_and_delivery(
                    Payload,
                    Hash,
                    ApiMsgId0,
                    MsgGuid,
                    DeliveryId,
                    ProductKey,
                    TopicTemplate,
                    DeviceNames
                )
            of
                {ok, ApiMsgId, _Delivery} ->
                    ok = count_offline(1, Offline),
                    submit_qos1_reserved(Targets, Payload, DeliveryId, ProductKey),
                    {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)};
                {error, _} ->
                    emqx_bcast_deliver:release_reservation(length(Targets)),
                    {ok, 500, #{},
                        emqx_bcast_api:error_response(
                            RequestId, <<"InternalError">>, <<"Storage error">>
                        )}
            end
    end;
deliver_qos1({reuse, ApiMsgId, MsgGuid}, DeviceNames, ProductKey, TopicTemplate, RequestId) ->
    emqx_bcast_metrics:qos1_in(),
    emqx_bcast_metrics:qos1_wanted(length(DeviceNames)),
    {Targets, Offline} = resolve(DeviceNames, ProductKey, TopicTemplate),
    case emqx_bcast_deliver:try_reserve(length(Targets)) of
        {error, overloaded} ->
            queue_full_response(RequestId);
        ok ->
            DeliveryId = emqx_bcast_utils:gen_guid(),
            _Delivery = emqx_bcast_storage:create_delivery(
                DeliveryId,
                MsgGuid,
                ProductKey,
                TopicTemplate,
                DeviceNames,
                length(DeviceNames)
            ),
            %% Best-effort TTL refresh; skipped silently when the queue is full.
            _ = emqx_bcast_deliver:submit_task(fun() ->
                _ = emqx_bcast_storage:refresh_message_ttl(MsgGuid),
                ok
            end),
            {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
            Payload = Msg#bcast_message.payload,
            ok = count_offline(1, Offline),
            submit_qos1_reserved(Targets, Payload, DeliveryId, ProductKey),
            {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, ApiMsgId)}
    end.

%% Submit chunks for a delivery whose queue slots were reserved upfront via
%% emqx_bcast_deliver:try_reserve/1.
submit_qos1_reserved(Targets, Payload, DeliveryId, ProductKey) ->
    ok = emqx_bcast_deliver:submit_targets_reserved(
        Targets, qos1_ctx(Payload, DeliveryId, ProductKey)
    ).

qos1_ctx(Payload, DeliveryId, ProductKey) ->
    Config = persistent_term:get({?APP, config}, #{}),
    ForceUpgrade = maps:get(force_upgrade_qos, Config, true),
    #{
        qos => 1,
        payload => Payload,
        delivery_id => DeliveryId,
        product_key => ProductKey,
        force_upgrade_qos => ForceUpgrade
    }.

queue_full_response(RequestId) ->
    {ok, 429, #{},
        emqx_bcast_api:error_response(
            RequestId, <<"DeliveryQueueFull">>, <<"Delivery queue is full, retry later">>
        )}.

%% Resolve devices to node-local delivery targets: pid, concrete topic and
%% matched subscription QoS. Pure ETS reads, so it is cheap enough to run
%% synchronously in the API handler. Devices that are not connected to this
%% node, or whose subscription does not match the target topic, are skipped
%% here and counted as offline by the caller.
resolve_local(DeviceNames, ProductKey, TopicTemplate) ->
    lists:filtermap(
        fun(DN) ->
            case emqx_bcast:lookup_device({ProductKey, DN}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DN),
                    case emqx_bcast_subscription:match(DN, Topic) of
                        {ok, SubQos} ->
                            {true, {DN, Pid, Topic, SubQos}};
                        false ->
                            false
                    end;
                {error, not_found} ->
                    false
            end
        end,
        DeviceNames
    ).

%% Resolve across the cluster: each node checks its own node-local device
%% and subscription ETS. Returns {Targets, OfflineCount}.
resolve(DeviceNames, ProductKey, TopicTemplate) ->
    Self = node(),
    Targets =
        lists:concat([
            case Node of
                Self ->
                    resolve_local(DeviceNames, ProductKey, TopicTemplate);
                _ ->
                    case
                        emqx_rpc:call(
                            Node,
                            ?MODULE,
                            resolve_local,
                            [DeviceNames, ProductKey, TopicTemplate]
                        )
                    of
                        {badrpc, _} -> [];
                        L when is_list(L) -> L
                    end
            end
         || Node <- emqx:running_nodes()
        ]),
    {Targets, length(DeviceNames) - length(Targets)}.

count_offline(_Qos, 0) -> ok;
count_offline(0, N) -> emqx_bcast_metrics:qos0_skipped(N);
count_offline(1, N) -> emqx_bcast_metrics:qos1_stored_offline(N).

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
