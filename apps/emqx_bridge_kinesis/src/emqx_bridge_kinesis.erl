%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, kinesis).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_kinesis".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields(connector_config) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, kinesis_action)),
            #{
                desc => <<"Kinesis Action Config">>,
                required => false
            }
        )};
fields(action_parameters) ->
    proplists:delete(local_topic, fields(producer));
fields(kinesis_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        ),
        #{
            resource_opts_ref => hoconsc:ref(?MODULE, action_resource_opts)
        }
    );
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields(
        _Overrides = [
            {batch_size, #{
                type => range(1, 500),
                validator => emqx_resource_validator:max(int, 500)
            }}
        ]
    );
fields("config_producer") ->
    emqx_bridge_schema:common_bridge_fields() ++
        fields("resource_opts") ++
        fields(connector_config) ++
        fields(producer);
fields("resource_opts") ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "creation_opts")
                }
            )}
    ];
fields("creation_opts") ->
    emqx_resource_schema:create_opts([
        {batch_size, #{
            type => range(1, 500),
            validator => emqx_resource_validator:max(int, 500)
        }}
    ]);
fields(connector_config) ->
    [
        {aws_access_key_id,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("aws_access_key_id")
                }
            )},
        {aws_secret_access_key,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    desc => ?DESC("aws_secret_access_key")
                }
            )},
        {endpoint,
            mk(
                binary(),
                #{
                    required => true,
                    example => <<"https://kinesis.us-east-1.amazonaws.com">>,
                    desc => ?DESC("endpoint")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    required => false,
                    default => 2,
                    desc => ?DESC("max_retries")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )}
    ];
fields(producer) ->
    [
        {payload_template,
            sc(
                emqx_schema:template(),
                #{
                    default => <<"${.}">>,
                    desc => ?DESC("payload_template")
                }
            )},
        {stream_name,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("stream_name")
                }
            )},
        {partition_key,
            sc(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("partition_key")
                }
            )}
    ] ++ fields(local_topic);
fields(local_topic) ->
    [
        {local_topic,
            sc(
                binary(),
                #{
                    desc => ?DESC("local_topic")
                }
            )}
    ];
fields("get_producer") ->
    emqx_bridge_schema:status_fields() ++ fields("post_producer");
fields("post_producer") ->
    [type_field_producer(), name_field() | fields("config_producer")];
fields("put_producer") ->
    fields("config_producer");
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields(connector_config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(kinesis_action)).

desc("config_producer") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("config_connector") ->
    ?DESC("config_connector");
desc(kinesis_action) ->
    ?DESC("kinesis_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

conn_bridge_examples(_Method) ->
    [
        #{
            <<"kinesis_producer">> => #{
                summary => <<"Amazon Kinesis Producer Bridge">>,
                value => conn_bridge_values()
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"kinesis">> =>
                #{
                    summary => <<"Kinesis Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        <<"aws_access_key_id">> => <<"your_access_key">>,
        <<"aws_secret_access_key">> => <<"aws_secret_key">>,
        <<"endpoint">> => <<"http://localhost:4566">>,
        <<"max_retries">> => 2,
        <<"pool_size">> => 8
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"kinesis">> =>
                #{
                    summary => <<"Kinesis Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        parameters => #{
            <<"partition_key">> => <<"any_key">>,
            <<"payload_template">> => <<"${.}">>,
            <<"stream_name">> => <<"my_stream">>
        }
    }.

conn_bridge_values() ->
    #{
        enable => true,
        type => kinesis_producer,
        name => <<"foo">>,
        aws_access_key_id => <<"aws_access_key_id">>,
        aws_secret_access_key => <<"******">>,
        endpoint => <<"https://kinesis.us-east-1.amazonaws.com">>,
        max_retries => 3,
        stream_name => <<"stream_name">>,
        partition_key => <<"key">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => 15000,
            query_mode => async,
            inflight_window => 100,
            max_buffer_bytes => 100 * 1024 * 1024
        }
    }.

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

enum(OfSymbols) -> hoconsc:enum(OfSymbols).

ref(Module, Name) -> hoconsc:ref(Module, Name).

type_field_producer() ->
    {type, mk(enum([kinesis_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
