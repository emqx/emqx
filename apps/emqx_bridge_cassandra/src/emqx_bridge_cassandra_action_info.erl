%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_cassandra_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(SCHEMA_MODULE, emqx_bridge_cassandra).

action_type_name() -> cassandra.

connector_type_name() -> cassandra.

schema_module() -> ?SCHEMA_MODULE.
