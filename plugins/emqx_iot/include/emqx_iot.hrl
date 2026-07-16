%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_IOT_HRL).
-define(EMQX_IOT_HRL, true).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(APP, emqx_iot).
-define(IOT_DELIVERY_ID, iot_delivery_id).
-define(IOT_MQ_REGISTRY, iot_mq).

-record(iot_mq_message, {
    msg_id :: binary(),
    api_msg_id :: binary(),
    content_hash :: binary(),
    payload :: binary(),
    created_at :: non_neg_integer(),
    expires_at :: non_neg_integer()
}).

-record(iot_mq_message_api_id, {
    api_msg_id :: binary(),
    msg_id :: binary()
}).

-record(iot_mq_message_hash, {
    hash :: binary(),
    msg_id :: binary()
}).

-record(iot_mq_msg, {
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

-record(iot_mq_msg_index, {
    key :: {binary(), binary()},
    delivery_ids :: [binary()]
}).

-record(iot_mq_device_sub, {
    key :: {binary(), binary()},
    clientid :: binary(),
    pid :: pid()
}).

-record(iot_mq_device_client, {
    clientid :: binary(),
    pk_dn :: {binary(), binary()},
    pid :: pid()
}).

-endif.
