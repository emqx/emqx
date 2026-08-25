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
    case parse_limit(maps:get(<<"limit">>, QS, undefined)) of
        {error, _} ->
            RequestId = emqx_bcast_utils:gen_api_uuid(),
            {error, 400, #{},
                emqx_bcast_api:error_response(
                    RequestId,
                    <<"InvalidParams">>,
                    <<"limit must be between 1 and 1000">>
                )};
        {ok, Limit} ->
            Cursor = parse_cursor(maps:get(<<"cursor">>, QS, undefined)),
            {Messages, NextCursor} = emqx_bcast_storage:list_messages(Limit, Cursor),
            Resp0 = #{<<"Messages">> => [message_json(M) || M <- Messages]},
            Resp =
                case NextCursor of
                    undefined -> Resp0;
                    {Created, MsgId} -> Resp0#{<<"Cursor">> => cursor_str(Created, MsgId)}
                end,
            ok_response(Resp)
    end;
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
            is_binary(ProductKey),
            ProductKey =/= <<>>,
            is_binary(DeviceName),
            DeviceName =/= <<>>
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

delivery_json(
    #bcast_msg{
        delivery_id = DeliveryId,
        product_key = ProductKey,
        target_ack_count = Target,
        counter = Counter,
        device_names = DeviceNames,
        created_at = CreatedAt,
        expires_at = ExpiresAt
    },
    ApiMsgId
) ->
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

%% limit: optional, default 100, max 1000. Values outside 1..1000 -> 400.
parse_limit(undefined) ->
    {ok, ?DEFAULT_LIMIT};
parse_limit(Bin) when is_binary(Bin) ->
    case string:to_integer(Bin) of
        {N, <<>>} when N >= 1, N =< ?MAX_LIMIT -> {ok, N};
        _ -> {error, invalid_limit}
    end;
parse_limit(_) ->
    {error, invalid_limit}.

%% cursor: opaque "<created_at>_<msg_id_hex>". Unknown/empty -> start.
parse_cursor(undefined) ->
    undefined;
parse_cursor(<<>>) ->
    undefined;
parse_cursor(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<"_">>) of
        [CreatedBin, MsgIdHex] ->
            case catch {binary_to_integer(CreatedBin), binary:decode_hex(MsgIdHex)} of
                {Created, MsgId} when is_integer(Created), is_binary(MsgId) ->
                    {Created, MsgId};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end;
parse_cursor(_) ->
    undefined.

cursor_str(Created, MsgId) ->
    <<(integer_to_binary(Created))/binary, "_", (binary:encode_hex(MsgId))/binary>>.
