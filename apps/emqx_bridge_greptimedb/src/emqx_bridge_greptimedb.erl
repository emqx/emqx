%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb).

-behaviour(emqx_connector_examples).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

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

-export([action_parameters_validator/1]).

-define(CONNECTOR_TYPE, greptimedb).
-define(ACTION_TYPE, greptimedb).

%% -------------------------------------------------------------------------------------------------
%% api

bridge_v2_examples(Method) ->
    WriteExample = [
        #{
            measurement => <<"mqtt">>,
            tags => [
                #{key => <<"clientid">>, value => <<"${clientid}">>}
            ],
            fields => [
                #{key => <<"temperature">>, type => float, value => <<"${payload.temperature}">>},
                #{key => <<"count">>, type => integer, value => <<"${payload.count}">>}
            ],
            timestamp => #{
                value => <<"${timestamp}">>,
                source_precision => ms
            }
        }
    ],
    ParamsExample = #{
        parameters => #{
            write => WriteExample, precision => ms
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

connector_values(_Method) ->
    #{
        type => greptimedb,
        name => <<"demo">>,
        enable => true,
        resource_opts => #{
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        username => <<"example_username">>,
        password => <<"******">>,
        dbname => <<"example_db">>,
        ttl => <<"3 years">>,
        server => <<"127.0.0.1:4001">>,
        ssl => #{enable => false}
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_greptimedb".

roots() -> [].

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
            required => true,
            validator => fun action_parameters_validator/1,
            desc => ?DESC(action_parameters)
        }),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {write_syntax, fun write_syntax/1},
        {write,
            mk(
                hoconsc:array(ref(?MODULE, greptimedb_write_point)),
                #{
                    required => false,
                    validator => fun emqx_schema:non_empty_array/1,
                    desc => ?DESC("write")
                }
            )},
        emqx_bridge_greptimedb_connector:precision_field()
    ];
fields(greptimedb_write_point) ->
    [
        {measurement,
            mk(emqx_schema:template(), #{
                required => true,
                validator => fun emqx_schema:non_empty_string/1,
                desc => ?DESC("write_measurement")
            })},
        {tags,
            mk(
                hoconsc:array(ref(?MODULE, greptimedb_write_tag)),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC("write_tags")
                }
            )},
        {fields,
            mk(
                hoconsc:array(ref(?MODULE, greptimedb_write_field)),
                #{
                    required => true,
                    validator => fun emqx_schema:non_empty_array/1,
                    desc => ?DESC("write_fields")
                }
            )},
        {timestamp,
            mk(
                hoconsc:union([
                    hoconsc:enum([false]),
                    ref(?MODULE, greptimedb_write_timestamp)
                ]),
                #{
                    required => false,
                    desc => ?DESC("write_timestamp")
                }
            )}
    ];
fields(greptimedb_write_tag) ->
    [
        {key,
            mk(emqx_schema:template(), #{
                required => true,
                validator => fun emqx_schema:non_empty_string/1,
                desc => ?DESC("write_tag_key")
            })},
        {value,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC("write_tag_value")
            })}
    ];
fields(greptimedb_write_field) ->
    [
        {key,
            mk(emqx_schema:template(), #{
                required => true,
                validator => fun emqx_schema:non_empty_string/1,
                desc => ?DESC("write_field_key")
            })},
        {type,
            mk(
                hoconsc:enum([auto, string, integer, float, boolean]),
                #{
                    required => true,
                    desc => ?DESC("write_field_type")
                }
            )},
        {value,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC("write_field_value")
            })}
    ];
fields(greptimedb_write_timestamp) ->
    [
        {value,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC("write_timestamp_value")
            })},
        {source_precision,
            mk(
                hoconsc:enum([s, ms, us, ns]),
                #{
                    required => true,
                    desc => ?DESC("write_timestamp_source_precision")
                }
            )}
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 100}},
        {batch_time, #{default => <<"100ms">>}}
    ]);
%% Connectors
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    emqx_bridge_greptimedb_connector:fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
%$ Bridge v2
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(greptimedb_action)).

desc("config") ->
    ?DESC("desc_config");
desc(greptimedb) ->
    ?DESC(emqx_bridge_greptimedb_connector, "greptimedb");
desc(greptimedb_action) ->
    ?DESC(greptimedb_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(greptimedb_write_point) ->
    ?DESC(greptimedb_write_point);
desc(greptimedb_write_tag) ->
    ?DESC(greptimedb_write_tag);
desc(greptimedb_write_field) ->
    ?DESC(greptimedb_write_field);
desc(greptimedb_write_timestamp) ->
    ?DESC(greptimedb_write_timestamp);
desc(action_resource_opts) ->
    emqx_bridge_v2_schema:desc(action_resource_opts);
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

action_parameters_validator(Conf) ->
    emqx_bridge_influxdb:action_parameters_validator(Conf).

write_syntax(type) ->
    emqx_bridge_influxdb:write_syntax_type();
write_syntax(required) ->
    false;
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

ref(StructName) -> hoconsc:ref(?MODULE, StructName).
