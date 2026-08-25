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

%% Core-local Mnesia tables:
%%   bcast_message        -- one row per registered message payload
%%   bcast_message_api_id -- maps the API-facing MessageId (UUID string) to msg_id
%%   bcast_message_hash   -- maps SHA-256 content hash to msg_id (dedup)
%%   bcast_msg            -- one row per BatchPub QoS=1 delivery call
%%   bcast_msg_index      -- per-device pending delivery queue
%% Node-local ETS tables (not in Mnesia):
%%   bcast_device_sub     -- online device -> {ProductKey, DeviceName}
%%   bcast_buffer_a/b     -- active/inactive delivery buffers in pull_pool
%%   bcast_buffer3        -- want_next dedup staging in pull_pool
%%   bcast_pull_inflight  -- claim-in-flight guard (window=1)
%%
%% Subscription state is NOT mirrored: delivery decisions read
%% emqx_broker:subscriptions(ChannelPid) directly from EMQX

-record(bcast_message, {
    msg_id :: binary(),
    api_msg_id :: binary(),
    content_hash :: binary(),
    payload :: binary(),
    delivery_count :: non_neg_integer(),
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

-type bcast_delivery_state() :: stored | {pending, PendingTs :: non_neg_integer()}.
-type bcast_index_entry() :: {DeliveryId :: binary(), bcast_delivery_state()}.

-record(bcast_device_sub, {
    key :: {binary(), binary()},
    clientid :: binary(),
    pid :: pid()
}).

-record(bcast_buffer_entry, {
    clientid :: binary(),
    delivery_id :: binary(),
    product_key :: binary(),
    topic_template :: binary(),
    payload :: binary(),
    pid :: pid()
}).

-record(bcast_buffer3, {
    clientid :: binary(),
    product_key :: binary(),
    pid :: pid()
}).

-endif.
