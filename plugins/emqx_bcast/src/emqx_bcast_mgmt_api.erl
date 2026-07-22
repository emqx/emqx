%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_mgmt_api).

-export([handle/3]).

-include("emqx_bcast.hrl").

-define(DEFAULT_LIMIT, 100).
-define(MAX_LIMIT, 1000).

handle(get, [<<"messages">>], Request) ->
    QS = maps:get(query_string, Request, #{}),
    Limit = parse_qs_int(maps:get(<<"limit">>, QS, undefined), ?DEFAULT_LIMIT, ?MAX_LIMIT),
    Offset = parse_qs_int(maps:get(<<"offset">>, QS, undefined), 0, infinity),
    {Total, Messages} = emqx_bcast_storage:list_messages(Limit, Offset),
    ok_response(#{
        <<"TotalCount">> => Total,
        <<"Messages">> => [message_json(M) || M <- Messages]
    });
handle(get, [<<"messages">>, ApiMsgId], _Request) ->
    case emqx_bcast_storage:get_message_by_api_id(ApiMsgId) of
        {ok, Msg, DeliveryCount} ->
            ok_response(
                (message_json(Msg))#{<<"DeliveryCount">> => DeliveryCount}
            );
        {error, not_found} ->
            not_found(<<"MessageNotFound">>, <<"Message does not exist">>)
    end;
handle(delete, [<<"messages">>, ApiMsgId], _Request) ->
    case emqx_bcast_storage:delete_message(ApiMsgId) of
        ok ->
            ok_response(#{});
        {error, not_found} ->
            not_found(<<"MessageNotFound">>, <<"Message does not exist">>)
    end;
handle(get, [<<"deliveries">>], Request) ->
    QS = maps:get(query_string, Request, #{}),
    case {maps:get(<<"product_key">>, QS, undefined), maps:get(<<"device_name">>, QS, undefined)} of
        {ProductKey, DeviceName} when
            is_binary(ProductKey), ProductKey =/= <<>>,
            is_binary(DeviceName), DeviceName =/= <<>>
        ->
            {ok, Deliveries} = emqx_bcast_storage:deliveries_for_device(ProductKey, DeviceName),
            ok_response(#{
                <<"Deliveries">> => [delivery_json(D, ApiId) || {D, ApiId} <- Deliveries]
            });
        _ ->
            RequestId = emqx_bcast_utils:gen_api_uuid(),
            {error, 400, #{},
                emqx_bcast_api:error_response(
                    RequestId,
                    <<"InvalidParams">>,
                    <<"product_key and device_name query parameters are required">>
                )}
    end;
handle(get, [<<"deliveries">>, IdStr], _Request) ->
    case emqx_bcast_utils:uuid_to_guid(IdStr) of
        {ok, DeliveryId} ->
            case emqx_bcast_storage:get_delivery(DeliveryId) of
                {ok, D, ApiMsgId} ->
                    ok_response(delivery_json(D, ApiMsgId));
                {error, not_found} ->
                    not_found(<<"DeliveryNotFound">>, <<"Delivery does not exist">>)
            end;
        error ->
            not_found(<<"DeliveryNotFound">>, <<"Delivery does not exist">>)
    end;
handle(delete, [<<"deliveries">>, IdStr], _Request) ->
    case emqx_bcast_utils:uuid_to_guid(IdStr) of
        {ok, DeliveryId} ->
            case emqx_bcast_storage:delete_delivery(DeliveryId) of
                ok ->
                    ok_response(#{});
                {error, not_found} ->
                    not_found(<<"DeliveryNotFound">>, <<"Delivery does not exist">>)
            end;
        error ->
            not_found(<<"DeliveryNotFound">>, <<"Delivery does not exist">>)
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

message_json(#bcast_message{
    api_msg_id = ApiMsgId,
    payload = Payload,
    created_at = CreatedAt,
    expires_at = ExpiresAt
}) ->
    #{
        <<"MessageId">> => ApiMsgId,
        <<"CreatedAt">> => CreatedAt,
        <<"ExpiresAt">> => ExpiresAt,
        <<"PayloadSize">> => byte_size(Payload)
    }.

delivery_json(#bcast_msg{
    delivery_id = DeliveryId,
    product_key = ProductKey,
    target_ack_count = Target,
    counter = Counter,
    device_names = DeviceNames,
    created_at = CreatedAt,
    expires_at = ExpiresAt
}, ApiMsgId) ->
    #{
        <<"DeliveryId">> => emqx_bcast_utils:guid_to_uuid(DeliveryId),
        <<"MessageId">> => ApiMsgId,
        <<"ProductKey">> => ProductKey,
        <<"DeviceNames">> => DeviceNames,
        <<"TargetCount">> => Target,
        <<"PendingCount">> => Target - Counter,
        <<"CreatedAt">> => CreatedAt,
        <<"ExpiresAt">> => ExpiresAt
    }.

ok_response(Data) ->
    RequestId = emqx_bcast_utils:gen_api_uuid(),
    {ok, 200, #{}, Data#{<<"Success">> => true, <<"RequestId">> => RequestId}}.

not_found(Code, Message) ->
    RequestId = emqx_bcast_utils:gen_api_uuid(),
    {error, 404, #{}, emqx_bcast_api:error_response(RequestId, Code, Message)}.

parse_qs_int(undefined, Default, _Max) ->
    Default;
parse_qs_int(Bin, Default, Max) when is_binary(Bin) ->
    case string:to_integer(Bin) of
        {N, <<>>} when N >= 0 -> cap(N, Max);
        _ -> Default
    end;
parse_qs_int(_, Default, _Max) ->
    Default.

cap(N, infinity) -> N;
cap(N, Max) when N > Max -> Max;
cap(N, _Max) -> N.
