%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_BCAST_HRL).
-define(EMQX_BCAST_HRL, true).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(APP, emqx_bcast).
-define(BCAST_DELIVERY_ID, bcast_delivery_id).
-define(BCAST_PRODUCT_KEY, bcast_product_key).
-define(BCAST_REGISTRY, bcast).

-define(TAB_MSG, bcast_message).
-define(TAB_MSG_API_ID, bcast_message_api_id).
-define(TAB_MSG_HASH, bcast_message_hash).
-define(TAB_MSG_REG, bcast_message_reg).
-define(TAB_MSG_REC, bcast_msg).
-define(TAB_MSG_META, bcast_msg_meta).
-define(TAB_MSG_META_CNT, bcast_msg_meta_counter).
-define(TAB_MSG_IDX, bcast_msg_index).
-define(TAB_QUOTA, bcast_quota).

%% Node-local intake queue (owned by emqx_bcast_intake; the L1 acceptance
%% cache: entries live here between the HTTP 200 and the mria promotion).
-define(TAB_INT_Q, bcast_intake_queue).
-define(TAB_INT_SEQ, bcast_intake_seq).

%% Shared global pending-count row held by emqx_bcast_index_owner (the
%% quota owner's node; update_counter from every shard). The per-device
%% index itself lives in the shards' process heaps, rebuildable from
%% bcast_msg (the mria tables stay authoritative).
-define(TAB_QUOTA_ETS, bcast_quota_ets).
-define(TAB_DEV_REGISTRY, bcast_device_registry).

%% mria rlog shard hosting the storage tables: writes happen on core
%% nodes (transactions), replicants receive async copies for local reads.
-define(BCAST_SHARD, emqx_bcast_shard).

-define(BCAST_RPC_CALL_TIMEOUT_MS, 15000).
-define(BCAST_TABLE_WAIT_MS, 15000).
-define(BCAST_API_RPC_TIMEOUT_MS, 30000).
-define(BCAST_ENSURE_COPIES_MS, 30000).

%% Core-local storage tables (mria ram_copies, no disk persistence;
%% pending deliveries survive single-core crashes via the peer core's copy,
%% but a full cluster restart drops them):
%%   bcast_message        -- one row per registered message payload
%%   bcast_message_api_id -- maps the API-facing MessageId (UUID string) to msg_id
%%   bcast_message_hash   -- maps SHA-256 content hash to msg_id (dedup)
%%   bcast_msg            -- one row per BatchPub QoS=1 delivery call
%%   bcast_msg_index      -- per-device pending queue (legacy layout; kept for
%%                           migration only, no longer written)
%%   bcast_quota          -- legacy global pending counter (migration only)
%% Node-local ETS tables (not in Mnesia):
%%   bcast_device_registry     -- {ProductKey, DeviceName} -> online channel pid;
%%                               not a subscription mirror
%%   bcast_client_state_<S> -- per-client pull state (claim round /
%%                             unacked window) for pull_shard partition S
%%
%% Subscription filters are cached per client in its pull state row
%% (topic_added/topic_removed from the session.subscribed /
%% session.unsubscribed hooks; fully re-read from emqx_broker on
%% session.resumed). The claim path reads the cache and falls back to
%% emqx_broker:subscriptions(ChannelPid) when the cache is empty.

-record(bcast_message, {
    msg_id :: binary(),
    api_msg_id :: binary(),
    content_hash :: binary(),
    payload :: binary(),
    %% Historical pending-delivery counter; no longer maintained (messages
    %% are GC'd by TTL via cleanup_expired). Kept at 0 for compatibility.
    delivery_count = 0 :: non_neg_integer(),
    created_at :: non_neg_integer(),
    expires_at :: non_neg_integer()
}).

-record(bcast_message_api_id, {
    api_msg_id :: binary(),
    msg_id :: binary()
}).

-record(bcast_message_hash, {
    hash :: binary(),
    msg_id :: binary()
}).

%% Set of messages explicitly pre-registered via RegisterMessage (or a
%% RegisterMessage refresh). Such messages must survive delivery_count
%% reaching zero and live until msg_ttl expiry. All message rows are
%% GC'd by TTL via cleanup_expired (delivery completion no longer
%% deletes them).
-record(bcast_message_reg, {
    msg_id :: binary(),
    registered_at :: non_neg_integer()
}).

-record(bcast_msg, {
    delivery_id :: binary(),
    msg_id :: binary(),
    product_key :: binary(),
    topic_template :: binary(),
    target_ack_count :: non_neg_integer(),
    counter :: non_neg_integer(),
    device_names :: [binary()],
    created_at :: non_neg_integer(),
    expires_at :: non_neg_integer()
}).

%% Per-request delivery metadata for the drain hot path: claims read
%% {msg_id, topic_template} from here instead of the full bcast_msg row
%% (which carries the whole device_names list, ~47KB at bs=1000), and acks
%% decrement the counter here (~200B dirty write vs the 47KB row rewrite).
%% Written in the same promotion transaction as bcast_msg; deleted on
%% completion, management delete or expiry.
-record(bcast_msg_meta, {
    delivery_id :: binary(),
    msg_id :: binary(),
    topic_template :: binary(),
    counter :: non_neg_integer()
}).

%% 3-tuple counter record for atomic mnesia:dirty_update_counter/3 from any
%% shard process. bcast_msg_meta stays as the info row; this row owns the
%% authoritative remaining-ack counter.
-record(bcast_msg_meta_counter, {
    delivery_id :: binary(),
    counter :: non_neg_integer()
}).

-record(bcast_msg_index, {
    key :: {ProductKey :: binary(), DeviceName :: binary()},
    deliveries :: [bcast_index_entry()],
    count :: non_neg_integer()
}).

%% Cluster-wide counter of pending index entries (one row, key = global).
%% Kept in mnesia so quota checks read a single record instead of scanning
%% the whole bcast_msg_index / bcast_msg tables.
-record(bcast_quota, {
    key :: global,
    count :: non_neg_integer()
}).

-type bcast_delivery_state() ::
    stored
    | {pending, PendingTs :: non_neg_integer()}
    %% claim_tag identifies the pull_shard flush generation that claimed the
    %% entry; timeout recovery can release exactly that claim without
    %% touching a newer concurrent claim.
    | {pending, PendingTs :: non_neg_integer(), ClaimTag :: pos_integer()}.
-type bcast_index_entry() :: {DeliveryId :: binary(), bcast_delivery_state()}.

-record(bcast_device_registry, {
    key :: {binary(), binary()},
    clientid :: binary(),
    pid :: pid()
}).

-define(BCAST_DEV_REGISTRY_OPTS, [
    named_table,
    public,
    set,
    {keypos, #bcast_device_registry.key},
    {read_concurrency, true},
    {write_concurrency, true}
]).

%% The only per-client state that survives after a delivery has been
%% handed to the channel process: enough to match the PUBACK exactly once
%% and to release the core claim if the client disappears before acking.
%% `ack_in_flight = true` means the PUBACK arrived and the core ack is in
%% flight; the row is then kept until the core-applied confirmation comes
%% back so `acked` is counted exactly once.
%% One per-client state row per pull_shard partition. Replaces the former
%% three tables (unacked marker / want_next stage / claim-inflight mark):
%% every client event costs one lookup instead of up to three, and the row
%% carries the per-device window (up to 1 unacked delivery) plus the
%% single in-flight claim round in one place, so the pull-side
%% state transitions stay atomic in one ETS write. The claim round is
%% written directly (no separate staged flag); the flush batches
%% not-yet-submitted claims from the gen_server's pending list.
-record(bcast_client_state, {
    key :: {binary(), binary()},
    product_key :: binary(),
    clientid :: binary(),
    pid :: pid(),
    %% claim round: {Tag, TsMillis}; at most one per client. Written
    %% directly on trigger/refill and cleared on commit/release.
    claim = undefined :: undefined | {pos_integer(), non_neg_integer()},
    %% committed-but-unacked deliveries in send order:
    %% [{DeliveryId, AckInFlight}] with length <= 1 (window=1)
    inflight = [] :: [{binary(), boolean()}],
    %% cached subscription filters [{TopicFilter, Qos}], maintained by the
    %% session.subscribed / session.unsubscribed / session.resumed hooks so
    %% the claim path reads them from this row instead of calling
    %% emqx_broker:subscriptions/1 per claim.
    topics = [] :: [{binary(), non_neg_integer()}]
}).

-endif.
