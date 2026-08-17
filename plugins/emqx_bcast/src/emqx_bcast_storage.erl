%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_storage).

%% Message records.
-export([
    create_message/4,
    lookup_message_by_hash/1,
    lookup_message/1,
    refresh_message_ttl/1,
    lookup_or_create_message/4,
    create_message_and_delivery/8,
    create_delivery/6
]).

%% Index / delivery state.
-export([
    add_index_entries/3,
    remove_index_entries/3,
    get_device_deliveries/1,
    get_device_delivery_entries/1
]).

%% Acking and claim (core authoritative paths).
-export([
    process_ack/3,
    process_ack_batch/1,
    claim_want_next_batch/1,
    release_claim/3
]).

%% Management and cleanup.
-export([
    list_messages/2,
    get_message_by_api_id/1,
    delete_message/1,
    get_delivery/1,
    deliveries_for_device/2,
    delete_delivery/1,
    cleanup_expired/0
]).

-include("emqx_bcast.hrl").

-define(TAB_MSG, bcast_message).
-define(TAB_MSG_API_ID, bcast_message_api_id).
-define(TAB_MSG_HASH, bcast_message_hash).
-define(TAB_MSG_REC, bcast_msg).
-define(TAB_MSG_IDX, bcast_msg_index).

%%--------------------------------------------------------------------
%% Message create / lookup
%%--------------------------------------------------------------------

lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    case
        transaction(fun() ->
            do_lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId, Now, TTL)
        end)
    of
        {atomic, Result} -> Result;
        {aborted, Reason} -> {error, Reason}
    end.

do_lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId, Now, TTL) ->
    case mnesia:wread({?TAB_MSG_HASH, Hash}) of
        [] ->
            Record = #bcast_message{
                msg_id = MsgId,
                api_msg_id = ApiMsgId,
                content_hash = Hash,
                payload = Payload,
                delivery_count = 0,
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
            [Existing] = mnesia:read(?TAB_MSG, ExistingMsgId, read),
            mnesia:write(Existing#bcast_message{expires_at = Now + TTL}),
            {existing, Existing#bcast_message.api_msg_id, ExistingMsgId}
    end.

%% Message dedup, delivery record creation and index insertion in one
%% transaction. This path runs only on core nodes.
create_message_and_delivery(
    Payload, Hash, ApiMsgId, MsgId, DeliveryId, ProductKey, TopicTemplate, DeviceNames
) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    case
        transaction(fun() ->
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
            add_index_entries_tx(ProductKey, DeviceNames, DeliveryId, stored),
            inc_delivery_count_tx(ResolvedMsgId),
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
        delivery_count = 0,
        created_at = Now,
        expires_at = Now + TTL
    },
    HashRecord = #bcast_message_hash{hash = Hash, msg_id = MsgId},
    ApiIdRecord = #bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId},
    {atomic, ok} = transaction(fun() ->
        mnesia:write(Record),
        mnesia:write(HashRecord),
        mnesia:write(ApiIdRecord)
    end),
    ok.

lookup_message_by_hash(Hash) ->
    case mnesia:dirty_read(?TAB_MSG_HASH, Hash) of
        [#bcast_message_hash{msg_id = MsgId}] ->
            lookup_message(MsgId);
        [] ->
            {error, not_found}
    end.

lookup_message(MsgId) ->
    case mnesia:dirty_read(?TAB_MSG, MsgId) of
        [#bcast_message{} = Msg] ->
            {ok, Msg};
        [] ->
            {error, not_found}
    end.

refresh_message_ttl(MsgId) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    transaction(fun() ->
        case mnesia:read(?TAB_MSG, MsgId, read) of
            [#bcast_message{} = Msg] ->
                mnesia:write(Msg#bcast_message{expires_at = Now + TTL}),
                ok;
            [] ->
                {error, not_found}
        end
    end).

%%--------------------------------------------------------------------
%% Deliveries and index
%%--------------------------------------------------------------------

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
    {atomic, ok} = transaction(fun() ->
        mnesia:write(Delivery),
        add_index_entries_tx(ProductKey, DeviceNames, DeliveryId, stored),
        inc_delivery_count_tx(MsgId)
    end),
    Delivery.

%% Index entries are {ProductKey, DeviceName} -> [{DeliveryId, State}].
%% The read-modify-write runs inside a transaction so concurrent deliveries
%% for the same device cannot overwrite each other.
add_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    {atomic, ok} = transaction(fun() ->
        add_index_entries_tx(ProductKey, DeviceNames, DeliveryId, stored)
    end),
    ok.

remove_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    {atomic, ok} = transaction(fun() ->
        lists:foreach(
            fun(DN) -> remove_index_entry_tx({ProductKey, DN}, DeliveryId) end,
            DeviceNames
        )
    end),
    ok.

add_index_entries_tx(ProductKey, DeviceNames, DeliveryId, State) ->
    lists:foreach(
        fun(DN) ->
            Key = {ProductKey, DN},
            Entries =
                case mnesia:wread({?TAB_MSG_IDX, Key}) of
                    [#bcast_msg_index{deliveries = Es}] -> Es;
                    [] -> []
                end,
            case lists:keymember(DeliveryId, 1, Entries) of
                true ->
                    ok;
                false ->
                    mnesia:write(#bcast_msg_index{
                        key = Key,
                        deliveries = [{DeliveryId, State} | Entries]
                    })
            end
        end,
        DeviceNames
    ).

remove_index_entry_tx(Key, DeliveryId) ->
    case mnesia:wread({?TAB_MSG_IDX, Key}) of
        [#bcast_msg_index{deliveries = Entries}] ->
            NewEntries = lists:keydelete(DeliveryId, 1, Entries),
            case NewEntries of
                [] ->
                    mnesia:delete({?TAB_MSG_IDX, Key});
                _ ->
                    mnesia:write(#bcast_msg_index{key = Key, deliveries = NewEntries})
            end;
        [] ->
            ok
    end.

get_device_deliveries({ProductKey, DeviceName}) ->
    case mnesia:dirty_read(?TAB_MSG_IDX, {ProductKey, DeviceName}) of
        [#bcast_msg_index{deliveries = Entries}] ->
            {ok, [DeliveryId || {DeliveryId, _State} <- Entries]};
        [] ->
            {ok, []}
    end.

get_device_delivery_entries({ProductKey, DeviceName}) ->
    case mnesia:dirty_read(?TAB_MSG_IDX, {ProductKey, DeviceName}) of
        [#bcast_msg_index{deliveries = Entries}] ->
            {ok, Entries};
        [] ->
            {ok, []}
    end.

%%--------------------------------------------------------------------
%% Acking
%%--------------------------------------------------------------------

process_ack(ProductKey, DeviceName, DeliveryId) ->
    case process_ack_batch([{ProductKey, DeviceName, DeliveryId}]) of
        [Result | _] -> Result;
        [] -> not_found
    end.

%% One transaction for the whole ack batch (F5).
process_ack_batch(Acks) ->
    case transaction(fun() -> process_ack_batch_tx(Acks, []) end) of
        {atomic, Results} -> Results;
        {aborted, Reason} -> [{error, Reason} || _ <- Acks]
    end.

process_ack_batch_tx([], Acc) ->
    lists:reverse(Acc);
process_ack_batch_tx([{ProductKey, DeviceName, DeliveryId} | Rest], Acc) ->
    Result = process_ack_one_tx(ProductKey, DeviceName, DeliveryId),
    process_ack_batch_tx(Rest, [Result | Acc]).

process_ack_one_tx(ProductKey, DeviceName, DeliveryId) ->
    Key = {ProductKey, DeviceName},
    case mnesia:wread({?TAB_MSG_IDX, Key}) of
        [#bcast_msg_index{deliveries = Entries}] ->
            case lists:keytake(DeliveryId, 1, Entries) of
                {value, {DeliveryId, _State}, NewEntries} ->
                    write_index_entries(Key, NewEntries),
                    case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
                        [#bcast_msg{counter = C, target_ack_count = T} = D] ->
                            NewC = C + 1,
                            case NewC >= T of
                                true ->
                                    mnesia:delete({?TAB_MSG_REC, DeliveryId}),
                                    %% Remove the remaining devices' index
                                    %% entries in the same transaction.
                                    lists:foreach(
                                        fun(DN) ->
                                            remove_index_entry_tx(
                                                {ProductKey, DN}, DeliveryId
                                            )
                                        end,
                                        D#bcast_msg.device_names
                                    ),
                                    dec_delivery_count_tx(D#bcast_msg.msg_id),
                                    counted;
                                false ->
                                    mnesia:write(D#bcast_msg{counter = NewC}),
                                    counted
                            end;
                        [] ->
                            counted
                    end;
                false ->
                    %% Already acked (or delivery completed): the entry is
                    %% gone, so a duplicate ACK does not count twice.
                    duplicate
            end;
        [] ->
            not_found
    end.

write_index_entries(Key, []) ->
    mnesia:delete({?TAB_MSG_IDX, Key});
write_index_entries(Key, NewEntries) ->
    mnesia:write(#bcast_msg_index{key = Key, deliveries = NewEntries}).

%% Track how many deliveries reference a message. When the last delivery
%% completes, delete the message records (payload, hash, api id). This is a
%% per-message decrement instead of a full bcast_msg scan on every delivery
%% completion, which kept ack batches from being O(table size).
inc_delivery_count_tx(MsgId) ->
    case mnesia:wread({?TAB_MSG, MsgId}) of
        [#bcast_message{delivery_count = N} = M] ->
            mnesia:write(M#bcast_message{delivery_count = N + 1});
        [] ->
            ok
    end.

dec_delivery_count_tx(MsgId) ->
    case mnesia:wread({?TAB_MSG, MsgId}) of
        [#bcast_message{delivery_count = N} = M] ->
            case N - 1 of
                0 ->
                    mnesia:delete({?TAB_MSG, MsgId}),
                    mnesia:delete({?TAB_MSG_HASH, M#bcast_message.content_hash}),
                    mnesia:delete({?TAB_MSG_API_ID, M#bcast_message.api_msg_id});
                Rest ->
                    mnesia:write(M#bcast_message{delivery_count = Rest})
            end;
        [] ->
            ok
    end.

%%--------------------------------------------------------------------
%% Want-next claim (core side)
%%--------------------------------------------------------------------

%% Entries :: [#{clientid := binary(), product_key := binary(),
%%               topics := [{binary(), non_neg_integer()}]}]
%% Returns :: [{ClientId, {ok, DeliverMap} | no_more}]
%% The claim runs on dirty ops instead of a transaction: a concurrent
%% claim/ack race on the same device can produce one duplicate delivery
%% (at-least-once, already accepted per arch doc F1). Skipping the
%% transaction manager keeps claim batches at dirty-read/write cost.
claim_want_next_batch(Entries) ->
    lists:map(
        fun(#{clientid := ClientId, product_key := ProductKey} = Entry) ->
            Topics = maps:get(topics, Entry, []),
            {ClientId, claim_one_dirty(ProductKey, ClientId, Topics)}
        end,
        Entries
    ).

claim_one_dirty(ProductKey, DeviceName, Topics) ->
    Key = {ProductKey, DeviceName},
    case mnesia:dirty_read(?TAB_MSG_IDX, Key) of
        [] ->
            no_more;
        [#bcast_msg_index{deliveries = Entries0}] ->
            claim_from_entries_dirty(Key, Entries0, Topics, ProductKey, DeviceName, [])
    end.

claim_from_entries_dirty(_Key, [], _Topics, _PK, _DN, _Kept) ->
    %% Nothing matched. Do not rewrite the index record: a later subscription
    %% change may make one of the stored entries deliverable.
    no_more;
claim_from_entries_dirty(Key, [{DeliveryId, State} | Rest], Topics, PK, DN, Kept) ->
    case mnesia:dirty_read(?TAB_MSG_REC, DeliveryId) of
        [#bcast_msg{msg_id = MsgId, topic_template = Template}] ->
            Topic = emqx_bcast_utils:expand_topic(Template, PK, DN),
            case topics_match(Topic, Topics) of
                true ->
                    case mnesia:dirty_read(?TAB_MSG, MsgId) of
                        [#bcast_message{payload = Payload}] ->
                            NewEntries = lists:reverse(Kept) ++ [{DeliveryId, pending} | Rest],
                            mnesia:dirty_write(#bcast_msg_index{key = Key, deliveries = NewEntries}),
                            {ok, #{
                                delivery_id => DeliveryId,
                                product_key => PK,
                                topic_template => Template,
                                payload => Payload,
                                claimed_state => State
                            }};
                        [] ->
                            %% Stale index: remove the entry and keep looking.
                            claim_from_entries_dirty(Key, Rest, Topics, PK, DN, Kept)
                    end;
                false ->
                    claim_from_entries_dirty(Key, Rest, Topics, PK, DN, [{DeliveryId, State} | Kept])
            end;
        [] ->
            %% Stale index entry: drop it and keep looking.
            claim_from_entries_dirty(Key, Rest, Topics, PK, DN, Kept)
    end.

topics_match(_Topic, []) ->
    false;
topics_match(Topic, [{Filter, _Qos} | Rest]) ->
    case emqx_topic:match(Topic, Filter) of
        true -> true;
        false -> topics_match(Topic, Rest)
    end.

%% Put a claimed delivery back to `stored` (used by pull_pool when the claim
%% arrived after the client unsubscribed, so no deliver was emitted).
release_claim(ProductKey, DeviceName, Did) ->
    Key = {ProductKey, DeviceName},
    {atomic, ok} = transaction(fun() ->
        case mnesia:wread({?TAB_MSG_IDX, Key}) of
            [#bcast_msg_index{deliveries = Entries}] ->
                NewEntries = lists:map(
                    fun
                        ({EntryId, pending}) when EntryId =:= Did -> {EntryId, stored};
                        (Entry) -> Entry
                    end,
                    Entries
                ),
                mnesia:write(#bcast_msg_index{key = Key, deliveries = NewEntries});
            [] ->
                ok
        end
    end),
    ok.

%%--------------------------------------------------------------------
%% Management queries
%%--------------------------------------------------------------------

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
    case mnesia:dirty_read(?TAB_MSG_API_ID, ApiMsgId) of
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
    case mnesia:dirty_read(?TAB_MSG_API_ID, ApiMsgId) of
        [] ->
            {error, not_found};
        [#bcast_message_api_id{msg_id = MsgId}] ->
            case
                transaction(fun() ->
                    Deliveries = mnesia:match_object(#bcast_msg{msg_id = MsgId, _ = '_'}),
                    lists:foreach(
                        fun(D) -> mnesia:delete({?TAB_MSG_REC, D#bcast_msg.delivery_id}) end,
                        Deliveries
                    ),
                    case mnesia:read(?TAB_MSG, MsgId, write) of
                        [#bcast_message{content_hash = Hash}] ->
                            mnesia:delete({?TAB_MSG, MsgId}),
                            mnesia:delete({?TAB_MSG_HASH, Hash}),
                            mnesia:delete({?TAB_MSG_API_ID, ApiMsgId});
                        [] ->
                            ok
                    end,
                    Deliveries
                end)
            of
                {atomic, Deliveries} ->
                    {atomic, ok} = transaction(fun() ->
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
    case mnesia:dirty_read(?TAB_MSG_REC, DeliveryId) of
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
    {ok, Entries} = get_device_delivery_entries({ProductKey, DeviceName}),
    Found = lists:filtermap(
        fun({Id, _State}) ->
            case get_delivery(Id) of
                {ok, D, ApiMsgId} -> {true, {D, ApiMsgId}};
                {error, not_found} -> false
            end
        end,
        Entries
    ),
    {ok, Found}.

delete_delivery(DeliveryId) ->
    case mnesia:dirty_read(?TAB_MSG_REC, DeliveryId) of
        [] ->
            {error, not_found};
        [#bcast_msg{msg_id = MsgId, product_key = ProductKey, device_names = DeviceNames}] ->
            {atomic, ok} = transaction(fun() ->
                mnesia:delete({?TAB_MSG_REC, DeliveryId}),
                lists:foreach(
                    fun(DN) -> remove_index_entry_tx({ProductKey, DN}, DeliveryId) end,
                    DeviceNames
                ),
                dec_delivery_count_tx(MsgId)
            end),
            ok
    end.

cleanup_expired() ->
    Now = emqx_bcast_utils:now_sec(),
    cleanup_expired_deliveries(Now),
    cleanup_expired_messages(Now).

cleanup_expired_deliveries(Now) ->
    Expired = mnesia:dirty_select(
        ?TAB_MSG_REC,
        [{#bcast_msg{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#bcast_msg{delivery_id = Did, msg_id = MsgId, device_names = DNs, product_key = PK}) ->
            {atomic, ok} = transaction(fun() ->
                mnesia:delete({?TAB_MSG_REC, Did}),
                lists:foreach(
                    fun(DN) -> remove_index_entry_tx({PK, DN}, Did) end,
                    DNs
                ),
                dec_delivery_count_tx(MsgId)
            end)
        end,
        Expired
    ).

cleanup_expired_messages(Now) ->
    Expired = mnesia:dirty_select(
        ?TAB_MSG,
        [{#bcast_message{expires_at = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}]
    ),
    lists:foreach(
        fun(#bcast_message{msg_id = Mid, content_hash = Hash, api_msg_id = ApiId}) ->
            {atomic, ok} = transaction(fun() ->
                mnesia:delete({?TAB_MSG, Mid}),
                mnesia:delete({?TAB_MSG_HASH, Hash}),
                mnesia:delete({?TAB_MSG_API_ID, ApiId})
            end)
        end,
        Expired
    ).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

transaction(Fun) ->
    mnesia:transaction(Fun).
