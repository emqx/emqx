%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rocketmq_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

bridge_v1_type_name() -> rocketmq.

action_type_name() -> rocketmq.

connector_type_name() -> rocketmq.

schema_module() -> emqx_bridge_rocketmq.
