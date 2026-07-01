%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_storage).

-export([
    create_message/4,
    lookup_message_by_hash/1,
    lookup_message/1,
    refresh_message_ttl/1,
    create_delivery/7,
    process_ack/3,
    get_device_deliveries/1,
    delete_delivery/1,
    cleanup_expired/0
]).

-include("emqx_iot.hrl").

create_message(ApiMsgId, MsgId, Hash, Payload) ->
    Now = emqx_iot_utils:now_sec(),
    TTL = emqx_iot_utils:ttl(),
    Record = #iot_mq_message{
        msg_id = MsgId,
        api_msg_id = ApiMsgId,
        content_hash = Hash,
        payload = Payload,
        created_at = Now,
        expires_at = Now + TTL
    },
    HashRecord = #iot_mq_message_hash{hash = Hash, msg_id = MsgId},
    ApiIdRecord = #iot_mq_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId},
    {atomic, ok} = mnesia:transaction(fun() ->
        mnesia:write(Record),
        mnesia:write(HashRecord),
        mnesia:write(ApiIdRecord)
    end),
    ok.

lookup_message_by_hash(Hash) ->
    case mnesia:dirty_read(iot_mq_message_hash, Hash) of
        [#iot_mq_message_hash{msg_id = MsgId}] ->
            lookup_message(MsgId);
        [] ->
            {error, not_found}
    end.

lookup_message(MsgId) ->
    case mnesia:dirty_read(iot_mq_message, MsgId) of
        [#iot_mq_message{} = Msg] ->
            {ok, Msg};
        [] ->
            {error, not_found}
    end.

refresh_message_ttl(MsgId) ->
    Now = emqx_iot_utils:now_sec(),
    TTL = emqx_iot_utils:ttl(),
    mnesia:transaction(fun() ->
        case lookup_message(MsgId) of
            {ok, Msg} ->
                mnesia:write(Msg#iot_mq_message{expires_at = Now + TTL}),
                ok;
            Error ->
                Error
        end
    end).

create_delivery(
    DeliveryId, MsgId, ProductKey, TopicTemplate, DeviceNames, TargetCount, ResponseTemplate
) ->
    Now = emqx_iot_utils:now_sec(),
    TTL = emqx_iot_utils:ttl(),
    Delivery = #iot_mq_msg{
        delivery_id = DeliveryId,
        msg_id = MsgId,
        product_key = ProductKey,
        topic_template = TopicTemplate,
        target_ack_count = TargetCount,
        counter = 0,
        device_names = DeviceNames,
        created_at = Now,
        expires_at = Now + TTL,
        response_topic_template = ResponseTemplate
    },
    {atomic, ok} = mnesia:transaction(fun() ->
        mnesia:write(Delivery),
        lists:foreach(
            fun(DN) ->
                Key = {ProductKey, DN},
                case mnesia:wread({iot_mq_msg_index, Key}) of
                    [#iot_mq_msg_index{delivery_ids = Ids} = Idx] ->
                        mnesia:write(Idx#iot_mq_msg_index{delivery_ids = Ids ++ [DeliveryId]});
                    [] ->
                        mnesia:write(#iot_mq_msg_index{key = Key, delivery_ids = [DeliveryId]})
                end
            end,
            DeviceNames
        )
    end),
    Delivery.

process_ack(ProductKey, DeviceName, DeliveryId) ->
    Key = {ProductKey, DeviceName},
    mnesia:transaction(fun() ->
        case mnesia:wread({iot_mq_msg_index, Key}) of
            [#iot_mq_msg_index{delivery_ids = Ids}] ->
                case lists:member(DeliveryId, Ids) of
                    true ->
                        NewIds = Ids -- [DeliveryId],
                        case NewIds of
                            [] ->
                                mnesia:delete({iot_mq_msg_index, Key});
                            _ ->
                                mnesia:write(#iot_mq_msg_index{key = Key, delivery_ids = NewIds})
                        end,
                        case mnesia:wread({iot_mq_msg, DeliveryId}) of
                            [#iot_mq_msg{counter = C, target_ack_count = T} = D] ->
                                NewC = C + 1,
                                case NewC >= T of
                                    true ->
                                        mnesia:delete({iot_mq_msg, DeliveryId});
                                    false ->
                                        mnesia:write(D#iot_mq_msg{counter = NewC})
                                end;
                            [] ->
                                ok
                        end;
                    false ->
                        ok
                end;
            [] ->
                ok
        end
    end).

get_device_deliveries({ProductKey, DeviceName}) ->
    case mnesia:dirty_read(iot_mq_msg_index, {ProductKey, DeviceName}) of
        [#iot_mq_msg_index{delivery_ids = Ids}] ->
            {ok, Ids};
        [] ->
            {ok, []}
    end.

delete_delivery(DeliveryId) ->
    mnesia:transaction(fun() ->
        mnesia:delete({iot_mq_msg, DeliveryId})
    end).

cleanup_expired() ->
    Now = emqx_iot_utils:now_sec(),
    cleanup_expired_deliveries(Now),
    cleanup_expired_messages(Now).

cleanup_expired_deliveries(Now) ->
    Expired = mnesia:dirty_select(
        iot_mq_msg,
        [{#iot_mq_msg{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#iot_mq_msg{delivery_id = Did, device_names = DNs, product_key = PK}) ->
            mnesia:transaction(fun() ->
                mnesia:delete({iot_mq_msg, Did}),
                lists:foreach(
                    fun(DN) ->
                        Key = {PK, DN},
                        case mnesia:wread({iot_mq_msg_index, Key}) of
                            [#iot_mq_msg_index{delivery_ids = Ids}] ->
                                NewIds = Ids -- [Did],
                                case NewIds of
                                    [] ->
                                        mnesia:delete({iot_mq_msg_index, Key});
                                    _ ->
                                        mnesia:write(#iot_mq_msg_index{
                                            key = Key, delivery_ids = NewIds
                                        })
                                end;
                            [] ->
                                ok
                        end
                    end,
                    DNs
                )
            end)
        end,
        Expired
    ).

cleanup_expired_messages(Now) ->
    Expired = mnesia:dirty_select(
        iot_mq_message,
        [{#iot_mq_message{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#iot_mq_message{msg_id = Mid, content_hash = Hash, api_msg_id = ApiId}) ->
            mnesia:transaction(fun() ->
                mnesia:delete({iot_mq_message, Mid}),
                mnesia:delete({iot_mq_message_hash, Hash}),
                mnesia:delete({iot_mq_message_api_id, ApiId})
            end)
        end,
        Expired
    ).
