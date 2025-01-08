%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(SCHEMA_MODULE, emqx_bridge_datalayers).

action_type_name() -> datalayers.

connector_type_name() -> datalayers.

schema_module() -> ?SCHEMA_MODULE.
