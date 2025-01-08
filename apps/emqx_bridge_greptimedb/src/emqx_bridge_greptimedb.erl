%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb).

-behaviour(emqx_connector_examples).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, greptimedb).
-define(ACTION_TYPE, greptimedb).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"greptimedb">> => #{
                summary => <<"Greptimedb HTTP API V2 Bridge">>,
                value => bridge_v1_values(Method)
            }
        }
    ].

bridge_v2_examples(Method) ->
    ParamsExample = #{
        parameters => #{
            write_syntax => write_syntax_value(), precision => ms
        }
    },
    [
        #{
            <<"greptimedb">> => #{
                summary => <<"GreptimeDB Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, greptimedb, greptimedb, ParamsExample
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"greptimedb">> => #{
                summary => <<"GreptimeDB Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, greptimedb, connector_values(Method)
                )
            }
        }
    ].

bridge_v1_values(_Method) ->
    #{
        type => greptimedb,
        name => <<"demo">>,
        enable => true,
        local_topic => <<"local/topic/#">>,
        write_syntax => write_syntax_value(),
        precision => ms,
        resource_opts => #{
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        username => <<"example_username">>,
        password => <<"******">>,
        dbname => <<"example_db">>,
        server => <<"127.0.0.1:4001">>,
        ssl => #{enable => false}
    }.

connector_values(Method) ->
    maps:without([write_syntax, precision], bridge_v1_values(Method)).

write_syntax_value() ->
    <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "bool=${payload.bool}">>.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_greptimedb".

roots() -> [].

fields("post_grpc_v1") ->
    method_fields(post, greptimedb);
fields("put_grpc_v1") ->
    method_fields(put, greptimedb);
fields("get_grpc_v1") ->
    method_fields(get, greptimedb);
fields(greptimedb = Type) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(Type);
%% Actions
fields(action) ->
    {greptimedb,
        mk(
            hoconsc:map(name, ref(?MODULE, greptimedb_action)),
            #{desc => <<"GreptimeDB Action Config">>, required => false}
        )};
fields(greptimedb_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        })
    );
fields(action_parameters) ->
    [
        {write_syntax, fun write_syntax/1},
        emqx_bridge_greptimedb_connector:precision_field()
    ];
%% Connectors
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_greptimedb_connector:fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        emqx_bridge_greptimedb_connector:fields("connector") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
%$ Bridge v2
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(greptimedb_action)).

method_fields(post, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType);
method_fields(get, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType) ++
        emqx_bridge_schema:status_fields();
method_fields(put, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType).

greptimedb_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {write_syntax, fun write_syntax/1}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields(Type) ->
    emqx_bridge_greptimedb_connector:fields(Type).

type_name_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Greptimedb using `", string:to_upper(Method), "` method."];
desc(greptimedb) ->
    ?DESC(emqx_bridge_greptimedb_connector, "greptimedb");
desc(greptimedb_action) ->
    ?DESC(greptimedb_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

write_syntax(type) ->
    emqx_bridge_influxdb:write_syntax_type();
write_syntax(required) ->
    true;
write_syntax(validator) ->
    [?NOT_EMPTY("the value of the field 'write_syntax' cannot be empty")];
write_syntax(converter) ->
    fun emqx_bridge_influxdb:to_influx_lines/1;
write_syntax(desc) ->
    ?DESC("write_syntax");
write_syntax(format) ->
    <<"sql">>;
write_syntax(_) ->
    undefined.
