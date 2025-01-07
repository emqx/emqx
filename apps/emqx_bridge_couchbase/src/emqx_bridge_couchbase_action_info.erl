%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_couchbase_action_info).

-behaviour(emqx_action_info).

-include("emqx_bridge_couchbase.hrl").

%% `emqx_action_info' API
-export([
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

action_type_name() -> ?ACTION_TYPE.

connector_type_name() -> ?CONNECTOR_TYPE.

schema_module() -> emqx_bridge_couchbase_action_schema.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
