%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_BCAST_HRL).
-define(EMQX_BCAST_HRL, true).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(APP, emqx_bcast).
-define(IOT_DELIVERY_ID, iot_delivery_id).
-define(BCAST_REGISTRY, bcast).

-record(bcast_message, {
    msg_id :: binary(),
    api_msg_id :: binary(),
    content_hash :: binary(),
    payload :: binary(),
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
    expires_at :: non_neg_integer(),
    response_topic_template :: binary() | undefined
}).

-record(bcast_msg_index, {
    key :: {binary(), binary()},
    delivery_ids :: [binary()]
}).

-record(bcast_device_sub, {
    key :: {binary(), binary()},
    clientid :: binary(),
    pid :: pid()
}).

-record(bcast_device_client, {
    clientid :: binary(),
    pk_dn :: {binary(), binary()},
    pid :: pid()
}).

-endif.

-record(bcast_subscription, {
    clientid :: binary(),
    pid :: pid(),
    topics :: [{binary(), non_neg_integer()}]
}).
