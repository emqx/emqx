%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql_action_info).

-behaviour(emqx_action_info).

%% behaviour callbacks
-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(MYSQL_TYPE, mysql).
-define(SCHEMA_MODULE, emqx_bridge_mysql).

action_type_name() -> ?MYSQL_TYPE.

connector_type_name() -> ?MYSQL_TYPE.

schema_module() -> ?SCHEMA_MODULE.
