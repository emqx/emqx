%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_opents_action_info).

-behaviour(emqx_action_info).

%% behaviour callbacks
-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(ACTION_TYPE, opents).
-define(SCHEMA_MODULE, emqx_bridge_opents).

action_type_name() -> ?ACTION_TYPE.

connector_type_name() -> ?ACTION_TYPE.

schema_module() -> ?SCHEMA_MODULE.
