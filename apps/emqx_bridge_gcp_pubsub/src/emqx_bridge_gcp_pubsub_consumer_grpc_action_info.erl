%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_action_info).

-behaviour(emqx_action_info).

-include("emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

%% `emqx_action_info' API
-export([
    is_source/0,
    is_action/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_action_info' API
%%------------------------------------------------------------------------------

is_source() -> true.

is_action() -> false.

action_type_name() -> ?SOURCE_TYPE.

connector_type_name() -> ?CONNECTOR_TYPE.

schema_module() -> emqx_bridge_gcp_pubsub_consumer_grpc_action_schema.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
