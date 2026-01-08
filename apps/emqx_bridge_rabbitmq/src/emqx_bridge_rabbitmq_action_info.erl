%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    is_source/0,
    is_action/0
]).

-define(SCHEMA_MODULE, emqx_bridge_rabbitmq_pubsub_schema).

action_type_name() -> rabbitmq.

connector_type_name() -> rabbitmq.

schema_module() -> ?SCHEMA_MODULE.

is_source() -> true.
is_action() -> true.
