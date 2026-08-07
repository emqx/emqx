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
    list_messages/2,
    get_message_by_api_id/1,
    delete_message/1,
    get_delivery/1,
    deliveries_for_device/2,
    delete_delivery/1,
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

%% Message dedup, delivery record creation and index insertion in one shard
%% transaction: the hash lookup-or-create is a read-modify-write, so it needs
%% a transactional write lock, and the index update must not lose concurrent
%% entries. One BatchPub QoS=1 call therefore costs one rlog round trip.
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
            add_index_entries_tx(ProductKey, DeviceNames, DeliveryId),
            {Status, ResolvedApiMsgId, Delivery}
        end)
    of
        {atomic, {_Status, ResolvedApiMsgId, Delivery}} ->
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
    mria:sync_dirty(?BCAST_SHARD, fun() ->
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
        mnesia:write(Delivery),
        add_index_entries_tx(ProductKey, DeviceNames, DeliveryId)
    end),
    Delivery.

%% Index entries are shard records: {ProductKey, DeviceName} -> [DeliveryId].
%% The read-modify-write runs inside a shard transaction so concurrent
%% deliveries for the same device cannot overwrite each other, and every node
%% sees the same index, so replay and ACK handling are cluster-consistent.
add_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
        add_index_entries_tx(ProductKey, DeviceNames, DeliveryId)
    end),
    ok.

remove_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
        lists:foreach(
            fun(DN) -> remove_index_entry_tx({ProductKey, DN}, DeliveryId) end,
            DeviceNames
        )
    end),
    ok.

add_index_entries_tx(ProductKey, DeviceNames, DeliveryId) ->
    lists:foreach(
        fun(DN) ->
            Key = {ProductKey, DN},
            Ids =
                case mnesia:wread({bcast_msg_index, Key}) of
                    [#bcast_msg_index{delivery_ids = Ids0}] -> Ids0;
                    [] -> []
                end,
            case lists:member(DeliveryId, Ids) of
                true ->
                    ok;
                false ->
                    mnesia:write(#bcast_msg_index{key = Key, delivery_ids = [DeliveryId | Ids]})
            end
        end,
        DeviceNames
    ).

remove_index_entry_tx(Key, DeliveryId) ->
    case mnesia:wread({bcast_msg_index, Key}) of
        [#bcast_msg_index{delivery_ids = Ids}] ->
            case Ids -- [DeliveryId] of
                Ids ->
                    ok;
                [] ->
                    mnesia:delete({bcast_msg_index, Key});
                NewIds ->
                    mnesia:write(#bcast_msg_index{key = Key, delivery_ids = NewIds})
            end;
        [] ->
            ok
    end.

process_ack(ProductKey, DeviceName, DeliveryId) ->
    Key = {ProductKey, DeviceName},
    Result =
        mria:transaction(?BCAST_SHARD, fun() ->
            case mnesia:wread({bcast_msg_index, Key}) of
                [#bcast_msg_index{delivery_ids = Ids}] ->
                    case lists:member(DeliveryId, Ids) of
                        true ->
                            remove_index_entry_tx(Key, DeliveryId),
                            case mnesia:wread({bcast_msg, DeliveryId}) of
                                [#bcast_msg{counter = C, target_ack_count = T} = D] ->
                                    NewC = C + 1,
                                    case NewC >= T of
                                        true ->
                                            mnesia:delete({bcast_msg, DeliveryId}),
                                            %% remove the remaining devices' index
                                            %% entries in the same transaction
                                            lists:foreach(
                                                fun(DN) ->
                                                    remove_index_entry_tx(
                                                        {ProductKey, DN}, DeliveryId
                                                    )
                                                end,
                                                D#bcast_msg.device_names
                                            ),
                                            counted;
                                        false ->
                                            mnesia:write(D#bcast_msg{counter = NewC}),
                                            counted
                                    end;
                                [] ->
                                    counted
                            end;
                        false ->
                            %% already acked (or delivery completed): the entry
                            %% is gone, so a duplicate ACK does not count twice
                            duplicate
                    end;
                [] ->
                    not_found
            end
        end),
    case Result of
        {atomic, AckResult} -> AckResult;
        {aborted, Reason} -> {error, Reason}
    end.

get_device_deliveries({ProductKey, DeviceName}) ->
    case mnesia:dirty_read(bcast_msg_index, {ProductKey, DeviceName}) of
        [#bcast_msg_index{delivery_ids = Ids}] ->
            {ok, Ids};
        [] ->
            {ok, []}
    end.

%% Management queries. Read paths use dirty operations; records are
%% replicated to every core node by the mria shard.

list_messages(Limit, Offset) ->
    All = mnesia:dirty_match_object(#bcast_message{_ = '_'}),
    Sorted = lists:reverse(lists:keysort(#bcast_message.created_at, All)),
    Page =
        case Offset >= length(Sorted) of
            true -> [];
            false -> lists:sublist(lists:nthtail(Offset, Sorted), Limit)
        end,
    {length(All), Page}.

get_message_by_api_id(ApiMsgId) ->
    case mnesia:dirty_read(bcast_message_api_id, ApiMsgId) of
        [#bcast_message_api_id{msg_id = MsgId}] ->
            case lookup_message(MsgId) of
                {ok, Msg} ->
                    Count = length(
                        mnesia:dirty_match_object(#bcast_msg{msg_id = MsgId, _ = '_'})
                    ),
                    {ok, Msg, Count};
                Error ->
                    Error
            end;
        [] ->
            {error, not_found}
    end.

delete_message(ApiMsgId) ->
    case mnesia:dirty_read(bcast_message_api_id, ApiMsgId) of
        [] ->
            {error, not_found};
        [#bcast_message_api_id{msg_id = MsgId}] ->
            case
                mria:transaction(?BCAST_SHARD, fun() ->
                    Deliveries = mnesia:match_object(#bcast_msg{msg_id = MsgId, _ = '_'}),
                    lists:foreach(
                        fun(D) -> mnesia:delete({bcast_msg, D#bcast_msg.delivery_id}) end,
                        Deliveries
                    ),
                    case mnesia:read(bcast_message, MsgId, write) of
                        [#bcast_message{content_hash = Hash}] ->
                            mnesia:delete({bcast_message, MsgId}),
                            mnesia:delete({bcast_message_hash, Hash}),
                            mnesia:delete({bcast_message_api_id, ApiMsgId});
                        [] ->
                            ok
                    end,
                    Deliveries
                end)
            of
                {atomic, Deliveries} ->
                    {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
                        lists:foreach(
                            fun(D) ->
                                lists:foreach(
                                    fun(DN) ->
                                        remove_index_entry_tx(
                                            {D#bcast_msg.product_key, DN},
                                            D#bcast_msg.delivery_id
                                        )
                                    end,
                                    D#bcast_msg.device_names
                                )
                            end,
                            Deliveries
                        )
                    end),
                    ok;
                {aborted, Reason} ->
                    {error, Reason}
            end
    end.

get_delivery(DeliveryId) ->
    case mnesia:dirty_read(bcast_msg, DeliveryId) of
        [#bcast_msg{} = D] ->
            ApiMsgId =
                case lookup_message(D#bcast_msg.msg_id) of
                    {ok, M} -> M#bcast_message.api_msg_id;
                    _ -> undefined
                end,
            {ok, D, ApiMsgId};
        [] ->
            {error, not_found}
    end.

deliveries_for_device(ProductKey, DeviceName) ->
    {ok, Ids} = get_device_deliveries({ProductKey, DeviceName}),
    Found = lists:filtermap(
        fun(Id) ->
            case get_delivery(Id) of
                {ok, D, ApiMsgId} -> {true, {D, ApiMsgId}};
                {error, not_found} -> false
            end
        end,
        Ids
    ),
    {ok, Found}.

delete_delivery(DeliveryId) ->
    case mnesia:dirty_read(bcast_msg, DeliveryId) of
        [] ->
            {error, not_found};
        [#bcast_msg{product_key = ProductKey, device_names = DeviceNames}] ->
            {atomic, ok} = mria:transaction(?BCAST_SHARD, fun() ->
                mnesia:delete({bcast_msg, DeliveryId}),
                lists:foreach(
                    fun(DN) -> remove_index_entry_tx({ProductKey, DN}, DeliveryId) end,
                    DeviceNames
                )
            end),
            ok
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
                mnesia:delete({bcast_msg, Did}),
                lists:foreach(
                    fun(DN) -> remove_index_entry_tx({PK, DN}, Did) end,
                    DNs
                )
            end)
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
