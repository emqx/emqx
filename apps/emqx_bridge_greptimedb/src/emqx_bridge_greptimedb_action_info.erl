%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(SCHEMA_MODULE, emqx_bridge_greptimedb).
-define(GREPTIMEDB_TYPE, greptimedb).

action_type_name() -> ?GREPTIMEDB_TYPE.

connector_type_name() -> ?GREPTIMEDB_TYPE.

schema_module() -> ?SCHEMA_MODULE.
