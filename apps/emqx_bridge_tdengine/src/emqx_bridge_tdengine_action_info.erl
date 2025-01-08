%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tdengine_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(ACTION_TYPE, tdengine).
-define(SCHEMA_MODULE, emqx_bridge_tdengine).

bridge_v1_type_name() ->
    ?ACTION_TYPE.

action_type_name() ->
    ?ACTION_TYPE.

connector_type_name() ->
    ?ACTION_TYPE.

schema_module() ->
    ?SCHEMA_MODULE.
