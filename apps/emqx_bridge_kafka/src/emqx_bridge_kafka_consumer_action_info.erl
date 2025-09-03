%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_action_info).

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

action_type_name() -> kafka_consumer.

connector_type_name() -> kafka_consumer.

schema_module() -> emqx_bridge_kafka_consumer_schema.
