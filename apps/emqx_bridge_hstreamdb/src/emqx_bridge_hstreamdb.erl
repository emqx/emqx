%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_hstreamdb).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_hstreamdb.hrl").

-import(hoconsc, [mk/2, enum/1]).

-export([
    conn_bridge_examples/1,
    bridge_v2_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, hstreamdb).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"hstreamdb">> => #{
                summary => <<"HStreamDB Bridge">>,
                value => conn_bridge_example_values(Method)
            }
        }
    ].

conn_bridge_example_values(get) ->
    conn_bridge_example_values(post);
conn_bridge_example_values(put) ->
    conn_bridge_example_values(post);
conn_bridge_example_values(post) ->
    #{
        type => <<"hstreamdb">>,
        name => <<"demo">>,
        direction => <<"egress">>,
        url => <<"http://127.0.0.1:6570">>,
        stream => <<"stream">>,
        %% raw HRecord
        record_template =>
            <<"{ \"temperature\": ${payload.temperature}, \"humidity\": ${payload.humidity} }">>,
        pool_size => 8,
        %% grpc_timeout => <<"1m">>
        resource_opts => #{
            query_mode => sync,
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        ssl => #{enable => false}
    };
conn_bridge_example_values(_) ->
    #{}.

connector_examples(Method) ->
    [
        #{
            <<"hstreamdb">> =>
                #{
                    summary => <<"HStreamDB Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        <<"url">> => <<"http://127.0.0.1:6570">>,
        <<"grpc_timeout">> => <<"30s">>,
        <<"ssl">> =>
            #{
                <<"enable">> => false,
                <<"verify">> => <<"verify_peer">>
            },
        <<"resource_opts">> =>
            #{
                <<"health_check_interval">> => <<"15s">>,
                <<"start_timeout">> => <<"5s">>
            }
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"hstreamdb">> =>
                #{
                    summary => <<"HStreamDB Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        <<"parameters">> => #{
            <<"partition_key">> => <<"hej">>,
            <<"record_template">> => <<"${payload}">>,
            <<"stream">> => <<"mqtt_message">>,
            <<"aggregation_pool_size">> => ?DEFAULT_AGG_POOL_SIZE,
            <<"writer_pool_size">> => ?DEFAULT_WRITER_POOL_SIZE
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_hstreamdb".

roots() -> [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields(connector_fields) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(hstreamdb_action));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, hstreamdb_action)),
            #{
                desc => <<"HStreamDB Action Config">>,
                required => false
            }
        )};
fields(hstreamdb_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields(action_parameters) ->
    [
        {stream,
            mk(binary(), #{
                required => true, desc => ?DESC(emqx_bridge_hstreamdb_connector, "stream_name")
            })},

        {partition_key,
            mk(emqx_schema:template(), #{
                required => false,
                desc => ?DESC(emqx_bridge_hstreamdb_connector, "partition_key")
            })},

        {grpc_flush_timeout, fun grpc_flush_timeout/1},
        {record_template, record_template_schema()},
        {aggregation_pool_size,
            mk(pos_integer(), #{
                default => ?DEFAULT_AGG_POOL_SIZE, desc => ?DESC("aggregation_pool_size")
            })},
        {max_batches,
            mk(pos_integer(), #{default => ?DEFAULT_MAX_BATCHES, desc => ?DESC("max_batches")})},
        {writer_pool_size,
            mk(pos_integer(), #{
                default => ?DEFAULT_WRITER_POOL_SIZE, desc => ?DESC("writer_pool_size")
            })},
        {batch_size, mk(pos_integer(), #{default => 100, desc => ?DESC("batch_size")})},
        {batch_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => ?DEFAULT_BATCH_INTERVAL_RAW, desc => ?DESC("batch_interval")
            })}
    ];
fields(connector_fields) ->
    [
        {url,
            mk(binary(), #{
                required => true,
                desc => ?DESC(emqx_bridge_hstreamdb_connector, "url"),
                default => <<"http://127.0.0.1:6570">>
            })},
        {grpc_timeout, fun grpc_timeout/1}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields(connector_fields) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    hstream_bridge_common_fields() ++
        connector_fields();
fields("post") ->
    hstream_bridge_common_fields() ++
        connector_fields() ++
        type_name_fields();
fields("get") ->
    hstream_bridge_common_fields() ++
        connector_fields() ++
        type_name_fields() ++
        emqx_bridge_schema:status_fields();
fields("put") ->
    hstream_bridge_common_fields() ++
        connector_fields().

record_template_schema() ->
    mk(emqx_schema:template(), #{
        default => <<"${payload}">>,
        desc => ?DESC("record_template")
    }).

grpc_timeout(type) -> emqx_schema:timeout_duration_ms();
grpc_timeout(desc) -> ?DESC(emqx_bridge_hstreamdb_connector, "grpc_timeout");
grpc_timeout(default) -> ?DEFAULT_GRPC_TIMEOUT_RAW;
grpc_timeout(required) -> false;
grpc_timeout(_) -> undefined.

grpc_flush_timeout(type) -> emqx_schema:timeout_duration_ms();
grpc_flush_timeout(desc) -> ?DESC("grpc_flush_timeout");
grpc_flush_timeout(default) -> ?DEFAULT_GRPC_FLUSH_TIMEOUT_RAW;
grpc_flush_timeout(required) -> false;
grpc_flush_timeout(_) -> undefined.

hstream_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})},
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {record_template, record_template_schema()}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields() ->
    emqx_bridge_hstreamdb_connector:fields(config).

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for HStreamDB bridge using `", string:to_upper(Method), "` method."];
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("config_connector") ->
    ?DESC("config_connector");
desc(hstreamdb_action) ->
    ?DESC("hstreamdb_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal
type_name_fields() ->
    [
        {type, mk(enum([hstreamdb]), #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].
