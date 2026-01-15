%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_emqx_tables_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_emqx_tables.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_connector_examples' API
-export([
    connector_examples/1
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() ->
    "connector_emqx_tables".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_bridge_greptimedb:fields("config_connector");
fields(connector_config) ->
    emqx_bridge_greptimedb:fields(connector_config).

desc("config_connector") ->
    ?DESC("config_connector");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"emqx_tables">> => #{
                summary => <<"EMQX Tables Connector">>,
                value => emqx_bridge_greptimedb:connector_examples(Method)
            }
        }
    ].

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
