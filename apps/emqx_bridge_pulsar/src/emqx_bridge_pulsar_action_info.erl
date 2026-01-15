%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pulsar_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    is_action/1
]).

is_action(_) -> true.

action_type_name() -> pulsar.

connector_type_name() -> pulsar.

schema_module() -> emqx_bridge_pulsar_pubsub_schema.
