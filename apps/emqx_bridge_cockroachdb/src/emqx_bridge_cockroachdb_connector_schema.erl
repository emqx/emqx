%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_cockroachdb_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_cockroachdb.hrl").

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
    "connector_cockroachdb".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_postgresql_connector_schema:fields("config_connector");
fields(connector_config) ->
    emqx_postgresql_connector_schema:fields("connection_fields") ++
        emqx_connector_schema:resource_opts_ref(emqx_postgresql_connector_schema, resource_opts).

desc("config_connector") ->
    ?DESC(emqx_postgresql_connector_schema, "config_connector");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"cockroachdb">> => #{
                summary => <<"CockroachDB Connector">>,
                value => emqx_postgresql_connector_schema:values({Method, ?CONNECTOR_TYPE_BIN})
            }
        }
    ].

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
