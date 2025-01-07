%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_timescale).

-behaviour(emqx_connector_examples).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Examples
-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, timescale).
-define(ACTION_TYPE, timescale).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"timescale">> => #{
                summary => <<"Timescale Bridge">>,
                value => emqx_bridge_pgsql:values_conn_bridge_examples(Method, timescale)
            }
        }
    ].

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_timescale".

roots() -> [].

fields("post") ->
    emqx_bridge_pgsql:fields("post", ?ACTION_TYPE, "config");
fields("config_connector") ->
    emqx_postgresql_connector_schema:fields("config_connector");
fields(action) ->
    {timescale,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_pgsql, pgsql_action)),
            #{
                desc => <<"Timescale Action Config">>,
                required => false
            }
        )};
fields("put_bridge_v2") ->
    emqx_bridge_pgsql:fields(pgsql_action);
fields("get_bridge_v2") ->
    emqx_bridge_pgsql:fields(pgsql_action);
fields("post_bridge_v2") ->
    emqx_bridge_pgsql:fields("post", ?ACTION_TYPE, pgsql_action);
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_postgresql_connector_schema:fields({Field, ?CONNECTOR_TYPE});
fields(Method) ->
    emqx_bridge_pgsql:fields(Method).

desc("config_connector") ->
    ?DESC(emqx_postgresql_connector_schema, "config_connector");
desc(_) ->
    undefined.

%% Examples

connector_examples(Method) ->
    [
        #{
            <<"timescale">> => #{
                summary => <<"Timescale Connector">>,
                value => emqx_postgresql_connector_schema:values({Method, <<"timescale">>})
            }
        }
    ].

bridge_v2_examples(Method) ->
    [
        #{
            <<"timescale">> => #{
                summary => <<"Timescale Action">>,
                value => emqx_bridge_pgsql:values({Method, timescale})
            }
        }
    ].
