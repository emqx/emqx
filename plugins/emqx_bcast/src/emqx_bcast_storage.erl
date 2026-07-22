%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_storage).

-export([
    create_message/4,
    lookup_message_by_hash/1,
    lookup_message/1,
    refresh_message_ttl/1,
    lookup_or_create_message/4,
    create_message_and_delivery/8,
    create_delivery/6,
    process_ack/3,
    get_device_deliveries/1,
    add_index_entries/3,
    remove_index_entries/3,
    cleanup_expired/0
]).

-include("emqx_bcast.hrl").

lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    case
        mria:transaction(?BCAST_SHARD, fun() ->
            do_lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId, Now, TTL)
        end)
    of
        {atomic, Result} -> Result;
        {aborted, Reason} -> {error, Reason}
    end.

do_lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId, Now, TTL) ->
    case mnesia:wread({bcast_message_hash, Hash}) of
        [] ->
            Record = #bcast_message{
                msg_id = MsgId,
                api_msg_id = ApiMsgId,
                content_hash = Hash,
                payload = Payload,
                created_at = Now,
                expires_at = Now + TTL
            },
            HashRecord = #bcast_message_hash{hash = Hash, msg_id = MsgId},
            ApiIdRecord = #bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId},
            mnesia:write(Record),
            mnesia:write(HashRecord),
            mnesia:write(ApiIdRecord),
            {created, ApiMsgId, MsgId};
        [#bcast_message_hash{msg_id = ExistingMsgId}] ->
            [Existing] = mnesia:read(bcast_message, ExistingMsgId, read),
            mnesia:write(Existing#bcast_message{expires_at = Now + TTL}),
            {existing, Existing#bcast_message.api_msg_id, ExistingMsgId}
    end.

%% Message dedup and delivery record creation in a single transaction,
%% so one BatchPub QoS=1 call costs one mria round trip instead of two.
create_message_and_delivery(
    Payload, Hash, ApiMsgId, MsgId, DeliveryId, ProductKey, TopicTemplate, DeviceNames
) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    case
        mria:transaction(?BCAST_SHARD, fun() ->
            {Status, ResolvedApiMsgId, ResolvedMsgId} = do_lookup_or_create_message(
                Payload, Hash, ApiMsgId, MsgId, Now, TTL
            ),
            Delivery = #bcast_msg{
                delivery_id = DeliveryId,
                msg_id = ResolvedMsgId,
                product_key = ProductKey,
                topic_template = TopicTemplate,
                target_ack_count = length(DeviceNames),
                counter = 0,
                device_names = DeviceNames,
                created_at = Now,
                expires_at = Now + TTL
            },
            mnesia:write(Delivery),
            {Status, ResolvedApiMsgId, ResolvedMsgId, Delivery}
        end)
    of
        {atomic, {_Status, ResolvedApiMsgId, _ResolvedMsgId, Delivery}} ->
            propagate_index_add(ProductKey, DeviceNames, DeliveryId),
            {ok, ResolvedApiMsgId, Delivery};
        {aborted, Reason} ->
            {error, Reason}
    end.

create_message(ApiMsgId, MsgId, Hash, Payload) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    Record = #bcast_message{
        msg_id = MsgId,
        api_msg_id = ApiMsgId,
        content_hash = Hash,
        payload = Payload,
        created_at = Now,
        expires_at = Now + TTL
    },
    HashRecord = #bcast_message_hash{hash = Hash, msg_id = MsgId},
    ApiIdRecord = #bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId},
    {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
        mnesia:write(Record),
        mnesia:write(HashRecord),
        mnesia:write(ApiIdRecord)
    end),
    ok.

lookup_message_by_hash(Hash) ->
    case mnesia:dirty_read(bcast_message_hash, Hash) of
        [#bcast_message_hash{msg_id = MsgId}] ->
            lookup_message(MsgId);
        [] ->
            {error, not_found}
    end.

lookup_message(MsgId) ->
    case mnesia:dirty_read(bcast_message, MsgId) of
        [#bcast_message{} = Msg] ->
            {ok, Msg};
        [] ->
            {error, not_found}
    end.

refresh_message_ttl(MsgId) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    mria:transaction(?BCAST_SHARD, fun() ->
        case lookup_message(MsgId) of
            {ok, Msg} ->
                mnesia:write(Msg#bcast_message{expires_at = Now + TTL}),
                ok;
            Error ->
                Error
        end
    end).

create_delivery(DeliveryId, MsgId, ProductKey, TopicTemplate, DeviceNames, TargetCount) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    Delivery = #bcast_msg{
        delivery_id = DeliveryId,
        msg_id = MsgId,
        product_key = ProductKey,
        topic_template = TopicTemplate,
        target_ack_count = TargetCount,
        counter = 0,
        device_names = DeviceNames,
        created_at = Now,
        expires_at = Now + TTL
    },
    {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
        mnesia:write(Delivery)
    end),
    propagate_index_add(ProductKey, DeviceNames, DeliveryId),
    Delivery.

%% Insert delivery ids into the node-local replay index on every node in the
%% cluster. The index maps {ProductKey, DeviceName} -> [DeliveryId]; without
%% cluster-wide propagation, a device reconnecting to a node other than the
%% API node would never replay the pending delivery.
propagate_index_add(ProductKey, DeviceNames, DeliveryId) ->
    add_index_entries(ProductKey, DeviceNames, DeliveryId),
    lists:foreach(
        fun(Node) ->
            emqx_rpc:cast(Node, ?MODULE, add_index_entries, [
                ProductKey, DeviceNames, DeliveryId
            ])
        end,
        emqx:running_nodes() -- [node()]
    ).

propagate_index_remove(ProductKey, DeviceNames, DeliveryId) ->
    remove_index_entries(ProductKey, DeviceNames, DeliveryId),
    lists:foreach(
        fun(Node) ->
            emqx_rpc:cast(Node, ?MODULE, remove_index_entries, [
                ProductKey, DeviceNames, DeliveryId
            ])
        end,
        emqx:running_nodes() -- [node()]
    ).

add_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    lists:foreach(
        fun(DN) -> add_index_entry({ProductKey, DN}, DeliveryId) end,
        DeviceNames
    ).

remove_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    lists:foreach(
        fun(DN) -> remove_index_entry({ProductKey, DN}, DeliveryId) end,
        DeviceNames
    ).

add_index_entry(Key, DeliveryId) ->
    Ids =
        case ets:lookup(bcast_msg_index, Key) of
            [#bcast_msg_index{delivery_ids = Ids0}] -> Ids0;
            [] -> []
        end,
    case lists:member(DeliveryId, Ids) of
        true ->
            ok;
        false ->
            ets:insert(bcast_msg_index, #bcast_msg_index{
                key = Key, delivery_ids = [DeliveryId | Ids]
            })
    end.

remove_index_entry(Key, DeliveryId) ->
    case ets:lookup(bcast_msg_index, Key) of
        [#bcast_msg_index{delivery_ids = Ids}] ->
            case lists:member(DeliveryId, Ids) of
                true ->
                    case Ids -- [DeliveryId] of
                        [] ->
                            ets:delete(bcast_msg_index, Key);
                        NewIds ->
                            ets:insert(bcast_msg_index, #bcast_msg_index{
                                key = Key, delivery_ids = NewIds
                            })
                    end;
                false ->
                    ok
            end;
        [] ->
            ok
    end.

process_ack(ProductKey, DeviceName, DeliveryId) ->
    Key = {ProductKey, DeviceName},
    case ets:lookup(bcast_msg_index, Key) of
        [#bcast_msg_index{delivery_ids = Ids}] ->
            case lists:member(DeliveryId, Ids) of
                true ->
                    remove_index_entry(Key, DeliveryId),
                    Result = mria:transaction(?BCAST_SHARD, fun() ->
                        case mnesia:wread({bcast_msg, DeliveryId}) of
                            [#bcast_msg{counter = C, target_ack_count = T} = D] ->
                                NewC = C + 1,
                                case NewC >= T of
                                    true ->
                                        mnesia:delete({bcast_msg, DeliveryId}),
                                        {completed, D#bcast_msg.device_names};
                                    false ->
                                        mnesia:write(D#bcast_msg{counter = NewC}),
                                        pending
                                end;
                            [] ->
                                missing
                        end
                    end),
                    case Result of
                        {atomic, {completed, DeviceNames}} ->
                            propagate_index_remove(ProductKey, DeviceNames, DeliveryId);
                        _ ->
                            ok
                    end,
                    counted;
                false ->
                    duplicate
            end;
        [] ->
            not_found
    end.

get_device_deliveries({ProductKey, DeviceName}) ->
    case ets:lookup(bcast_msg_index, {ProductKey, DeviceName}) of
        [#bcast_msg_index{delivery_ids = Ids}] ->
            {ok, Ids};
        [] ->
            {ok, []}
    end.

cleanup_expired() ->
    Now = emqx_bcast_utils:now_sec(),
    cleanup_expired_deliveries(Now),
    cleanup_expired_messages(Now).

cleanup_expired_deliveries(Now) ->
    Expired = mnesia:dirty_select(
        bcast_msg,
        [{#bcast_msg{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#bcast_msg{delivery_id = Did, device_names = DNs, product_key = PK}) ->
            {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
                mnesia:delete({bcast_msg, Did})
            end),
            propagate_index_remove(PK, DNs, Did)
        end,
        Expired
    ).

cleanup_expired_messages(Now) ->
    Expired = mnesia:dirty_select(
        bcast_message,
        [{#bcast_message{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#bcast_message{msg_id = Mid, content_hash = Hash, api_msg_id = ApiId}) ->
            {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
                mnesia:delete({bcast_message, Mid}),
                mnesia:delete({bcast_message_hash, Hash}),
                mnesia:delete({bcast_message_api_id, ApiId})
            end)
        end,
        Expired
    ).
