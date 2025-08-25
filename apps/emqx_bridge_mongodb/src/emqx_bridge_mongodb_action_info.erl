%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mongodb_action_info).

-behaviour(emqx_action_info).

%% behaviour callbacks
-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(SCHEMA_MODULE, emqx_bridge_mongodb).

action_type_name() -> mongodb.

connector_type_name() -> mongodb.

schema_module() -> ?SCHEMA_MODULE.
