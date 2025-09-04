%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_action_info).

-behaviour(emqx_action_info).

-export([
    is_source/0,
    is_action/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

is_source() -> true.

is_action() -> false.

action_type_name() -> gcp_pubsub_consumer.

connector_type_name() -> gcp_pubsub_consumer.

schema_module() -> emqx_bridge_gcp_pubsub_consumer_schema.
