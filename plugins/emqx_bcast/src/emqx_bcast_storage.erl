%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_storage).

%% Message records (authoritative mria state).
-export([
    create_message/4,
    lookup_message_by_hash/1,
    lookup_message/1,
    refresh_message_ttl/1,
    lookup_or_create_message/4
]).

%% Promotion primitives for the async intake path (emqx_bcast_promoter).
-export([promote_batch/1, promote_entry_tx/1]).

%% Delivery / index facade: the per-device pending index now lives in the
%% ETS tables owned by emqx_bcast_index_owner on the owner core. Every
%% index operation below routes to that single process (serialized, no
%% mnesia locks). The mnesia bcast_msg_index / bcast_quota tables remain
%% for compatibility but are no longer written.
-export([
    create_message_and_delivery/8,
    create_message_and_delivery_quota/9,
    create_delivery/6,
    add_index_entries/3,
    remove_index_entries/3,
    get_device_deliveries/1,
    get_device_delivery_entries/1,
    pending_delivery_count/0,
    pending_delivery_count_for/1
]).

%% Acking and claim (owner-routed).
-export([
    process_ack/3,
    process_ack_batch/1,
    claim_want_next_batch/1,
    release_claim/3,
    release_client_claims/3
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

%% Transaction helpers shared with the index owner (run inside the
%% owner's own transactions).
-export([inc_delivery_count_tx/1, dec_delivery_count_tx/1]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-type api_msg_id() :: binary().
-type msg_id() :: binary().
-type delivery_id() :: binary().
-type product_key() :: binary().
-type device_name() :: binary().

%%--------------------------------------------------------------------
%% Message create / lookup (unchanged: authoritative mria state)
%%--------------------------------------------------------------------

-spec lookup_or_create_message(binary(), binary(), api_msg_id(), msg_id()) ->
    {created | existing, api_msg_id(), msg_id()} | {error, term()}.
lookup_or_create_message(Payload, Hash, ApiMsgId, MsgId) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    case
        transaction(fun() ->
            {Status, ResolvedApiMsgId, ResolvedMsgId, NewMsg} = resolve_message_tx(
                Hash, ApiMsgId, MsgId, Now, TTL
            ),
            write_message_tx(NewMsg, Payload, Hash, ResolvedApiMsgId, ResolvedMsgId, Now, TTL),
            {Status, ResolvedApiMsgId, ResolvedMsgId}
        end)
    of
        {atomic, Result} -> Result;
        {aborted, Reason} -> {error, Reason}
    end.

-spec create_message(api_msg_id(), msg_id(), binary(), binary()) -> ok.
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

-spec lookup_message_by_hash(binary()) -> {ok, #bcast_message{}} | {error, not_found}.
lookup_message_by_hash(Hash) ->
    case mnesia:dirty_read(?TAB_MSG_HASH, Hash) of
        [#bcast_message_hash{msg_id = MsgId}] ->
            lookup_message(MsgId);
        [] ->
            {error, not_found}
    end.

-spec lookup_message(msg_id()) -> {ok, #bcast_message{}} | {error, not_found}.
lookup_message(MsgId) ->
    case mnesia:dirty_read(?TAB_MSG, MsgId) of
        [#bcast_message{} = Msg] ->
            {ok, Msg};
        [] ->
            {error, not_found}
    end.

-spec refresh_message_ttl(msg_id()) ->
    {atomic, ok | {error, not_found}} | {aborted, term()}.
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
%% Promotion primitives (async intake path)
%%--------------------------------------------------------------------

%% Promote one batch of intake entries with a single mnesia transaction:
%% per entry one hash resolve + message create/refresh and one delivery
%% row write. The number of mnesia rows per entry is independent of the
%% batch size (the per-device index is appended to ETS by the index owner
%% after the commit), which is what keeps promotion capacity above the
%% 1600 req/s target at any batch size.
%%
%% Idempotent: an entry whose delivery row already exists (crash between
%% commit and queue dequeue) resolves to `already_promoted` and is safe to
%% re-append to the ETS index (dedup by delivery id).
-spec promote_batch([map()]) -> {ok, [ok | already_promoted | {error, term()}]} | {error, term()}.
promote_batch(Entries) ->
    case transaction(fun() -> promote_batch_tx(Entries) end) of
        {atomic, Results} -> {ok, Results};
        {aborted, Reason} -> {error, Reason}
    end.

promote_batch_tx(Entries) ->
    %% Group entries by content hash: identical payloads (the loadtest
    %% sends the same 256B body for every request) share one message row,
    %% so the delivery_count bump and TTL refresh happen once per group
    %% instead of once per entry. With one hash for the whole run this
    %% turns N read-modify-write pairs on the same mnesia row into a
    %% single write, removing the cross-worker lock contention that capped
    %% promotion at ~520 req/s. Delivery rows stay per entry (idempotent:
    %% already-committed entries resolve to already_promoted).
    %% O(n) map grouping (lists:keyfind/keyreplace per entry was
    %% O(n x distinct-hash) inside the mnesia transaction, extending lock
    %% hold time).
    Groups = maps:to_list(
        lists:foldl(
            fun(Entry, Acc) ->
                Hash = maps:get(hash, Entry),
                maps:update_with(Hash, fun(List) -> [Entry | List] end, [Entry], Acc)
            end,
            #{},
            Entries
        )
    ),
    lists:append([promote_group_tx(Group) || {_Hash, Group} <- Groups]).

%% Promote one hash-group: one message create/refresh + delivery_count
%% bump for the whole group, then one delivery + meta row per entry.
%% Runs inside the promote_batch transaction.
promote_group_tx(Entries) ->
    {First, _Rest} =
        case Entries of
            [F | R] -> {F, R};
            [] -> {undefined, []}
        end,
    case First of
        undefined ->
            [];
        _ ->
            {_Status, ResolvedApiMsgId, ResolvedMsgId} = write_message_group_tx(
                First, length(Entries)
            ),
            GroupResults = [
                promote_entry_delivery_tx(Entry, ResolvedApiMsgId, ResolvedMsgId)
             || Entry <- Entries
            ],
            GroupResults
    end.

%% Hash resolve + one message create/refresh for the whole group; the
%% delivery_count bump is the group size (all entries share the message).
write_message_group_tx(Entry, GroupSize) ->
    Payload = maps:get(payload, Entry),
    Hash = maps:get(hash, Entry),
    ApiMsgId = maps:get(api_msg_id, Entry),
    MsgId = maps:get(msg_id, Entry),
    Now = maps:get(created_at, Entry),
    TTL = maps:get(expires_at, Entry) - Now,
    case mnesia:wread({?TAB_MSG_HASH, Hash}) of
        [] ->
            write_new_message_group_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL, GroupSize),
            {created, ApiMsgId, MsgId};
        [#bcast_message_hash{msg_id = ExistingMsgId}] ->
            case mnesia:wread({?TAB_MSG, ExistingMsgId}) of
                [#bcast_message{} = Existing] ->
                    mnesia:write(Existing#bcast_message{
                        delivery_count = Existing#bcast_message.delivery_count + GroupSize,
                        expires_at = Now + TTL
                    }),
                    {existing, Existing#bcast_message.api_msg_id, ExistingMsgId};
                [] ->
                    write_new_message_group_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL, GroupSize),
                    {created, ApiMsgId, MsgId}
            end
    end.

write_new_message_group_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL, GroupSize) ->
    mnesia:write(#bcast_message{
        msg_id = MsgId,
        api_msg_id = ApiMsgId,
        content_hash = Hash,
        payload = Payload,
        delivery_count = GroupSize,
        created_at = Now,
        expires_at = Now + TTL
    }),
    mnesia:write(#bcast_message_hash{hash = Hash, msg_id = MsgId}),
    mnesia:write(#bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId}).

%% Delivery + meta rows for one entry, using the group-resolved message.
promote_entry_delivery_tx(Entry, _ResolvedApiMsgId, ResolvedMsgId) ->
    DeliveryId = maps:get(delivery_id, Entry),
    case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
        [_] ->
            already_promoted;
        [] ->
            Delivery = #bcast_msg{
                delivery_id = DeliveryId,
                msg_id = ResolvedMsgId,
                product_key = maps:get(product_key, Entry),
                topic_template = maps:get(topic_template, Entry),
                target_ack_count = length(maps:get(devices, Entry)),
                counter = 0,
                device_names = maps:get(devices, Entry),
                created_at = maps:get(created_at, Entry),
                expires_at = maps:get(expires_at, Entry)
            },
            mnesia:write(Delivery),
            mnesia:write(#bcast_msg_meta{
                delivery_id = DeliveryId,
                msg_id = ResolvedMsgId,
                topic_template = maps:get(topic_template, Entry),
                counter = length(maps:get(devices, Entry))
            }),
            ok
    end.

%% Entry shape matches emqx_bcast_intake:entry(). Runs inside a mnesia
%% transaction (promoter batch or owner create_sync).
%% Idempotent: an already-committed delivery returns {error, already_exists}
%% instead of re-writing the rows and double-incrementing the message
%% delivery_count (a create_sync retry after an index-append failure used
%% to double-count; test/legacy path).
-spec promote_entry_tx(map()) -> {ok, api_msg_id(), #bcast_msg{}} | {error, term()}.
promote_entry_tx(Entry) ->
    DeliveryId = maps:get(delivery_id, Entry),
    case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
        [_] ->
            {error, already_exists};
        [] ->
            promote_entry_tx_new(Entry, DeliveryId)
    end.

promote_entry_tx_new(Entry, DeliveryId) ->
    Payload = maps:get(payload, Entry),
    Hash = maps:get(hash, Entry),
    ApiMsgId = maps:get(api_msg_id, Entry),
    MsgId = maps:get(msg_id, Entry),
    ProductKey = maps:get(product_key, Entry),
    TopicTemplate = maps:get(topic_template, Entry),
    Devices = maps:get(devices, Entry),
    CreatedAt = maps:get(created_at, Entry),
    ExpiresAt = maps:get(expires_at, Entry),
    %% Hash resolve, message create/refresh and the delivery_count bump are
    %% one step; dedup resolves to an existing message whose TTL is
    %% refreshed in the same write.
    {_Status, ResolvedApiMsgId, ResolvedMsgId} = write_message_inc_tx(
        Payload, Hash, ApiMsgId, MsgId, CreatedAt, ExpiresAt - CreatedAt
    ),
    Delivery = #bcast_msg{
        delivery_id = DeliveryId,
        msg_id = ResolvedMsgId,
        product_key = ProductKey,
        topic_template = TopicTemplate,
        target_ack_count = length(Devices),
        counter = 0,
        device_names = Devices,
        created_at = CreatedAt,
        expires_at = ExpiresAt
    },
    mnesia:write(Delivery),
    mnesia:write(#bcast_msg_meta{
        delivery_id = DeliveryId,
        msg_id = ResolvedMsgId,
        topic_template = TopicTemplate,
        counter = length(Devices)
    }),
    {ok, ResolvedApiMsgId, Delivery}.

%%--------------------------------------------------------------------
%% Delivery / index facade (routes to the ETS index owner)
%%--------------------------------------------------------------------

-spec create_message_and_delivery(
    binary(),
    binary(),
    api_msg_id(),
    msg_id(),
    delivery_id(),
    product_key(),
    binary(),
    [device_name()]
) -> {ok, api_msg_id(), #bcast_msg{}} | {error, term()}.
create_message_and_delivery(
    Payload, Hash, ApiMsgId, MsgId, DeliveryId, ProductKey, TopicTemplate, DeviceNames
) ->
    create_message_and_delivery_quota(
        Payload,
        Hash,
        ApiMsgId,
        MsgId,
        DeliveryId,
        ProductKey,
        TopicTemplate,
        DeviceNames,
        #{global => infinity, per_device => infinity}
    ).

-spec create_message_and_delivery_quota(
    binary(),
    binary(),
    api_msg_id(),
    msg_id(),
    delivery_id(),
    product_key(),
    binary(),
    [device_name()],
    #{global := non_neg_integer() | infinity, per_device := non_neg_integer() | infinity}
) -> {ok, api_msg_id(), #bcast_msg{}} | {error, term()}.
create_message_and_delivery_quota(
    Payload, Hash, ApiMsgId, MsgId, DeliveryId, ProductKey, TopicTemplate, DeviceNames, Quota
) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    Entry = #{
        payload => Payload,
        hash => Hash,
        api_msg_id => ApiMsgId,
        msg_id => MsgId,
        delivery_id => DeliveryId,
        product_key => ProductKey,
        topic_template => TopicTemplate,
        devices => DeviceNames,
        created_at => Now,
        expires_at => Now + TTL
    },
    emqx_bcast_index_owner:create_sync(Entry, Quota).

-spec create_delivery(
    delivery_id(), msg_id(), product_key(), binary(), [device_name()], non_neg_integer()
) -> {ok, #bcast_msg{}} | {error, term()}.
create_delivery(DeliveryId, MsgId, ProductKey, TopicTemplate, DeviceNames, TargetCount) ->
    emqx_bcast_index_owner:create_delivery(
        DeliveryId, MsgId, ProductKey, TopicTemplate, DeviceNames, TargetCount
    ).

-spec add_index_entries(product_key(), [device_name()], delivery_id()) -> ok.
add_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    _ = emqx_bcast_index_owner:append_batch(
        [{ProductKey, DN, DeliveryId} || DN <- DeviceNames]
    ),
    ok.

-spec remove_index_entries(product_key(), [device_name()], delivery_id()) -> ok.
remove_index_entries(ProductKey, DeviceNames, DeliveryId) ->
    _ = emqx_bcast_index_owner:remove_batch(
        [{ProductKey, DN, DeliveryId} || DN <- DeviceNames]
    ),
    ok.

-spec get_device_deliveries({product_key(), device_name()}) -> {ok, [delivery_id()]}.
get_device_deliveries(Key) ->
    emqx_bcast_index_owner:device_deliveries(Key).

-spec get_device_delivery_entries({product_key(), device_name()}) -> {ok, [bcast_index_entry()]}.
get_device_delivery_entries(Key) ->
    emqx_bcast_index_owner:device_delivery_entries(Key).

-spec pending_delivery_count() -> non_neg_integer().
pending_delivery_count() ->
    emqx_bcast_index_owner:pending_count().

-spec pending_delivery_count_for({product_key(), device_name()}) -> non_neg_integer().
pending_delivery_count_for(Key) ->
    emqx_bcast_index_owner:pending_count_for(Key).

%%--------------------------------------------------------------------
%% Acking and claim (owner-routed)
%%--------------------------------------------------------------------

-spec process_ack(product_key(), device_name(), delivery_id()) ->
    counted | duplicate | not_found | {error, term()}.
process_ack(ProductKey, DeviceName, DeliveryId) ->
    case process_ack_batch([{ProductKey, DeviceName, DeliveryId}]) of
        [Result | _] -> Result;
        [] -> not_found
    end.

-spec process_ack_batch([{product_key(), device_name(), delivery_id()}]) ->
    [counted | duplicate | not_found | {error, term()}].
process_ack_batch(Acks) ->
    try emqx_bcast_index_owner:ack_batch(Acks) of
        Results -> Results
    catch
        Error:Reason ->
            ?SLOG(warning, #{
                msg => "bcast_ack_owner_call_failed",
                exception => Error,
                reason => Reason
            }),
            [{error, Reason} || _ <- Acks]
    end.

-spec claim_want_next_batch([map()]) -> [{binary(), map() | no_more}] | {error, term()}.
claim_want_next_batch(Entries) ->
    try emqx_bcast_index_owner:claim(Entries) of
        Results -> Results
    catch
        Error:Reason ->
            ?SLOG(error, #{
                msg => "bcast_claim_owner_call_failed",
                exception => Error,
                reason => Reason
            }),
            {error, Reason}
    end.

-spec release_claim(product_key(), device_name(), delivery_id()) -> ok.
release_claim(ProductKey, DeviceName, Did) ->
    _ = emqx_bcast_index_owner:release_claim(ProductKey, DeviceName, Did),
    ok.

-spec release_client_claims(product_key(), device_name(), pos_integer()) -> ok | {error, term()}.
release_client_claims(ProductKey, DeviceName, ClaimTag) ->
    _ = emqx_bcast_index_owner:release_client_claims(ProductKey, DeviceName, ClaimTag),
    ok.

%%--------------------------------------------------------------------
%% Management queries
%%--------------------------------------------------------------------

%% Keyset pagination over messages ordered by (created_at, msg_id)
%% descending. The cursor is an opaque token encoding the last seen
%% (created_at, msg_id); messages are immutable so the order is stable
%% between pages.
-spec list_messages(pos_integer(), undefined | {non_neg_integer(), msg_id()}) ->
    {[#bcast_message{}], undefined | {non_neg_integer(), msg_id()}}.
list_messages(Limit, Cursor) ->
    All = mnesia:dirty_match_object(?TAB_MSG, #bcast_message{_ = '_'}),
    %% Sort by {created_at, msg_id} so messages created in the same second
    %% have a deterministic order (msg_id is unique).
    Sorted = lists:reverse(
        lists:sort(
            [
                {M#bcast_message.created_at, M#bcast_message.msg_id, M}
             || M <- All
            ]
        )
    ),
    case Cursor of
        undefined ->
            Page = lists:sublist(Sorted, Limit),
            {page_messages(Page), maybe_cursor(Page, Limit)};
        {LastCreated, LastMsgId} ->
            After = [
                Entry
             || Entry = {Created, MsgId, _M} <- Sorted,
                {Created, MsgId} < {LastCreated, LastMsgId}
            ],
            Page = lists:sublist(After, Limit),
            {page_messages(Page), maybe_cursor(Page, Limit)}
    end.

page_messages(Page) ->
    [M || {_Created, _MsgId, M} <- Page].

%% A cursor is only meaningful when the page is full: a short page means
%% there is nothing after it.
maybe_cursor(Page, Limit) when length(Page) < Limit ->
    undefined;
maybe_cursor([], _Limit) ->
    undefined;
maybe_cursor(Page, _Limit) ->
    {Created, MsgId, _M} = lists:last(Page),
    {Created, MsgId}.

-spec get_message_by_api_id(api_msg_id()) ->
    {ok, #bcast_message{}, non_neg_integer()} | {error, not_found}.
get_message_by_api_id(ApiMsgId) ->
    case mnesia:dirty_read(?TAB_MSG_API_ID, ApiMsgId) of
        [#bcast_message_api_id{msg_id = MsgId}] ->
            case lookup_message(MsgId) of
                {ok, Msg} ->
                    %% delivery_count is maintained transactionally and is
                    %% the authoritative value; a full bcast_msg scan here
                    %% turned one metadata GET into O(all deliveries).
                    {ok, Msg, Msg#bcast_message.delivery_count};
                Error ->
                    Error
            end;
        [] ->
            {error, not_found}
    end.

-spec delete_message(api_msg_id()) -> ok | {error, term()}.
delete_message(ApiMsgId) ->
    emqx_bcast_index_owner:delete_message(ApiMsgId).

-spec get_delivery(delivery_id()) ->
    {ok, #bcast_msg{}, api_msg_id() | undefined} | {error, not_found}.
get_delivery(DeliveryId) ->
    case mnesia:dirty_read(?TAB_MSG_REC, DeliveryId) of
        [#bcast_msg{} = D] ->
            Counter =
                case mnesia:dirty_read(?TAB_MSG_META, DeliveryId) of
                    [#bcast_msg_meta{counter = C}] -> C;
                    [] -> D#bcast_msg.counter
                end,
            ApiMsgId =
                case lookup_message(D#bcast_msg.msg_id) of
                    {ok, M} -> M#bcast_message.api_msg_id;
                    _ -> undefined
                end,
            {ok, D#bcast_msg{counter = Counter}, ApiMsgId};
        [] ->
            {error, not_found}
    end.

-spec deliveries_for_device(product_key(), device_name()) ->
    {ok, [{#bcast_msg{}, api_msg_id() | undefined}]}.
deliveries_for_device(ProductKey, DeviceName) ->
    {ok, Entries} = emqx_bcast_index_owner:device_delivery_entries({ProductKey, DeviceName}),
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

-spec delete_delivery(delivery_id()) -> ok | {error, term()}.
delete_delivery(DeliveryId) ->
    emqx_bcast_index_owner:delete_delivery(DeliveryId).

-spec cleanup_expired() -> ok.
cleanup_expired() ->
    emqx_bcast_index_owner:cleanup_expired().

%%--------------------------------------------------------------------
%% Shared transaction helpers (called by the index owner inside its own
%% mnesia transactions)
%%--------------------------------------------------------------------

-spec inc_delivery_count_tx(msg_id()) -> ok.
inc_delivery_count_tx(MsgId) ->
    case mnesia:wread({?TAB_MSG, MsgId}) of
        [#bcast_message{delivery_count = N} = M] ->
            mnesia:write(M#bcast_message{delivery_count = N + 1});
        [] ->
            ok
    end.

-spec dec_delivery_count_tx(msg_id()) -> ok.
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
%% Message tx internals
%%--------------------------------------------------------------------

%% Hash lookup only; decides whether the message record must be created
%% (NewMsg = true) or refreshed (false).
resolve_message_tx(Hash, ApiMsgId, MsgId, _Now, _TTL) ->
    case mnesia:wread({?TAB_MSG_HASH, Hash}) of
        [] ->
            {created, ApiMsgId, MsgId, true};
        [#bcast_message_hash{msg_id = ExistingMsgId}] ->
            case mnesia:wread({?TAB_MSG, ExistingMsgId}) of
                [#bcast_message{} = Existing] ->
                    {existing, Existing#bcast_message.api_msg_id, ExistingMsgId, false};
                [] ->
                    {created, ApiMsgId, MsgId, true}
            end
    end.

%% Hash resolve + message create/refresh + delivery_count increment in one
%% step. Callers must finish every delivery write before calling this.
write_message_inc_tx(Payload, Hash, ApiMsgId, MsgId, Now, TTL) ->
    case mnesia:wread({?TAB_MSG_HASH, Hash}) of
        [] ->
            write_new_message_inc_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL),
            {created, ApiMsgId, MsgId};
        [#bcast_message_hash{msg_id = ExistingMsgId}] ->
            case mnesia:wread({?TAB_MSG, ExistingMsgId}) of
                [#bcast_message{} = Existing] ->
                    mnesia:write(Existing#bcast_message{
                        delivery_count = Existing#bcast_message.delivery_count + 1,
                        expires_at = Now + TTL
                    }),
                    {existing, Existing#bcast_message.api_msg_id, ExistingMsgId};
                [] ->
                    %% The hash points at a message record that is already
                    %% gone; create a fresh one under the new id.
                    write_new_message_inc_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL),
                    {created, ApiMsgId, MsgId}
            end
    end.

write_new_message_inc_tx(MsgId, ApiMsgId, Hash, Payload, Now, TTL) ->
    mnesia:write(#bcast_message{
        msg_id = MsgId,
        api_msg_id = ApiMsgId,
        content_hash = Hash,
        payload = Payload,
        %% The delivery that triggered the creation counts immediately.
        delivery_count = 1,
        created_at = Now,
        expires_at = Now + TTL
    }),
    mnesia:write(#bcast_message_hash{hash = Hash, msg_id = MsgId}),
    mnesia:write(#bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId}).

write_message_tx(true, Payload, Hash, ApiMsgId, MsgId, Now, TTL) ->
    Record = #bcast_message{
        msg_id = MsgId,
        api_msg_id = ApiMsgId,
        content_hash = Hash,
        payload = Payload,
        delivery_count = 0,
        created_at = Now,
        expires_at = Now + TTL
    },
    mnesia:write(Record),
    mnesia:write(#bcast_message_hash{hash = Hash, msg_id = MsgId}),
    mnesia:write(#bcast_message_api_id{api_msg_id = ApiMsgId, msg_id = MsgId});
write_message_tx(false, _Payload, _Hash, _ApiMsgId, MsgId, Now, TTL) ->
    case mnesia:wread({?TAB_MSG, MsgId}) of
        [#bcast_message{} = Existing] ->
            mnesia:write(Existing#bcast_message{expires_at = Now + TTL});
        [] ->
            ok
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

transaction(Fun) ->
    %% Retry on lock clashes. mnesia re-runs the fun from scratch on a
    %% retry, so transaction semantics are preserved.
    mnesia:transaction(Fun, 20).
