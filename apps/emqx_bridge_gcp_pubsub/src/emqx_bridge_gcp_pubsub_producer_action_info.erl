%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_producer_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

action_type_name() -> gcp_pubsub_producer.

connector_type_name() -> gcp_pubsub_producer.

schema_module() -> emqx_bridge_gcp_pubsub_producer_schema.
