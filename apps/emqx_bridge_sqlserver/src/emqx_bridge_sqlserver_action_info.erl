%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_sqlserver_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(ACTION_TYPE, sqlserver).
-define(SCHEMA_MODULE, emqx_bridge_sqlserver).

action_type_name() -> ?ACTION_TYPE.

connector_type_name() -> ?ACTION_TYPE.

schema_module() -> ?SCHEMA_MODULE.
