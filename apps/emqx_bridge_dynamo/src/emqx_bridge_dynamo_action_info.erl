%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

bridge_v1_type_name() -> dynamo.

action_type_name() -> dynamo.

connector_type_name() -> dynamo.

schema_module() -> emqx_bridge_dynamo.
