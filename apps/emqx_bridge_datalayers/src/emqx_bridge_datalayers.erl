%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers).

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

-define(CONNECTOR_TYPE, datalayers).
-define(ACTION_TYPE, datalayers).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived) "
    "values (${id}, ${topic}, ${qos}, ${payload}, ${timestamp})"
>>).

-define(write_syntax_example, <<
    "${topic},clientid=${clientid} ",
    "payload=${payload},",
    "${clientid}_int_value=${payload.int_key}i,",
    "bool=${payload.bool}"
>>).

%% Examples
conn_bridge_examples(Method) ->
    [
        #{
            <<"datalayers">> => #{
                summary => <<"Datalayers Bridge">>,
                value => values("datalayers", Method)
            }
        }
    ].

bridge_v2_examples(Method) ->
    ParamsExampleInflux = #{
        parameters => #{
            write_syntax => ?write_syntax_example, precision => ms
        }
    },
    ParamsExampleArrowFlight = #{
        parameters => #{
            sql => ?DEFAULT_SQL
        }
    },
    [
        #{
            <<"datalayers_influx">> => #{
                summary => <<"Datalayers Action by InfluxDB Driver">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, datalayers, datalayers, ParamsExampleInflux
                )
            }
        },
        #{
            <<"datalayers_arrow_flight">> => #{
                summary => <<"Datalayers Action by Arrow Flight SQL Driver">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, datalayers, datalayers_arrow_flgiht, ParamsExampleArrowFlight
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"datalayers">> => #{
                summary => <<"Datalayers Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, datalayers, connector_values(datalayers_influx)
                )
            }
        },
        #{
            <<"datalayers_arrow_flight">> => #{
                summary => <<"Datalayers Connector by Arrow Flight SQL Driver">>,
                value => emqx_connector_schema:connector_values(
                    Method, datalayers, connector_values(datalayers_arrow_flight)
                )
            }
        }
    ].

connector_values(Type) ->
    maps:merge(
        basic_connector_values(),
        #{parameters => connector_values_v(Type)}
    ).

connector_values_v(Type) when
    Type =:= datalayers_influx orelse Type =:= datalayers_arrow_flight
->
    #{
        database => <<"example_database">>,
        username => <<"example_username">>,
        password => <<"******">>
    }.

basic_connector_values() ->
    #{
        enable => true,
        server => <<"127.0.0.1:8361">>,
        pool_size => 8,
        ssl => #{enable => false}
    }.

values(Protocol, get) ->
    values(Protocol, post);
values("datalayers", post) ->
    SupportUint = <<>>,
    TypeOpts = connector_values_v(datalayers),
    values(common, "datalayers", SupportUint, TypeOpts);
values(Protocol, put) ->
    values(Protocol, post).

values(common, Protocol, SupportUint, TypeOpts) ->
    CommonConfigs = #{
        type => list_to_atom(Protocol),
        name => <<"demo">>,
        enable => true,
        write_syntax =>
            <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
                "${clientid}_int_value=${payload.int_key}i,", SupportUint/binary,
                "bool=${payload.bool}">>,
        precision => ms,
        resource_opts => #{
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        server => <<"127.0.0.1:8361">>,
        ssl => #{enable => false}
    },
    maps:merge(TypeOpts, CommonConfigs).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_datalayers".

roots() -> [].

fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_datalayers_connector:fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(action) ->
    {datalayers,
        mk(
            hoconsc:map(name, ref(?MODULE, datalayers_action)),
            #{desc => <<"Datalayers Action Config">>, required => false}
        )};
fields(datalayers_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            hoconsc:union([
                ref(?MODULE, action_parameters_influx),
                ref(?MODULE, action_parameters_arrow_flight)
            ]),
            #{
                required => true, desc => ?DESC(action_parameters_influx)
            }
        )
    );
fields(action_parameters_influx) ->
    [
        {write_syntax, fun write_syntax/1},
        emqx_bridge_datalayers_connector:precision_field()
    ];
fields(action_parameters_arrow_flight) ->
    [
        {sql,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        emqx_bridge_datalayers_connector:fields("connector") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(datalayers_action)).

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Datalayers using `", string:to_upper(Method), "` method."];
desc(datalayers_api) ->
    ?DESC(emqx_bridge_datalayers_connector, "datalayers");
desc(datalayers_action) ->
    ?DESC(datalayers_action);
desc(action_parameters_influx) ->
    ?DESC(action_parameters_influx);
desc(action_parameters_arrow_flight) ->
    ?DESC(action_parameters_arrow_flight);
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
