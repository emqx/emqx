%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_emqx_tables_connector_info).

-behaviour(emqx_connector_info).

-include("emqx_bridge_emqx_tables.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `emqx_connector_info' API
-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SCHEMA_MOD, emqx_bridge_emqx_tables_connector_schema).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_connector_info' API
%%------------------------------------------------------------------------------

type_name() ->
    ?CONNECTOR_TYPE.

bridge_types() ->
    [?ACTION_TYPE].

resource_callback_module() ->
    emqx_bridge_greptimedb_connector.

config_schema() ->
    {?CONNECTOR_TYPE,
        hoconsc:mk(
            hoconsc:map(
                name,
                hoconsc:ref(
                    ?SCHEMA_MOD,
                    "config_connector"
                )
            ),
            #{
                desc => ?DESC(emqx_bridge_emqx_tables_connector_schema, "config_connector"),
                required => false
            }
        )}.

schema_module() ->
    ?SCHEMA_MOD.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        ?SCHEMA_MOD, ?CONNECTOR_TYPE_BIN, Method ++ "_connector"
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
