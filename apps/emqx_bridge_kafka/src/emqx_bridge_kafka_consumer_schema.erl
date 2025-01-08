%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_consumer_schema).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    source_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, kafka_consumer).
-define(SOURCE_TYPE, kafka_consumer).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "kafka_consumer".

roots() -> [].

%%=========================================
%% Source fields
%%=========================================
fields(source) ->
    {kafka_consumer,
        mk(
            hoconsc:map(name, ref(?MODULE, consumer_source)),
            #{
                desc => <<"Kafka Consumer Source Config">>,
                required => false
            }
        )};
fields(consumer_source) ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        mk(
            ref(?MODULE, source_parameters),
            #{
                required => true,
                desc => ?DESC(consumer_source)
            }
        )
    );
fields(source_parameters) ->
    Fields0 = emqx_bridge_kafka:fields(consumer_kafka_opts),
    Fields1 = emqx_bridge_kafka:fields(consumer_opts),
    Fields2 = proplists:delete(kafka, Fields1),
    Fields = lists:map(
        fun
            ({topic_mapping = Name, Sc}) ->
                %% to please dialyzer...
                Override = #{
                    type => hocon_schema:field_schema(Sc, type),
                    required => false,
                    default => [],
                    validator => fun legacy_consumer_topic_mapping_validator/1,
                    importance => ?IMPORTANCE_HIDDEN
                },
                {Name, hocon_schema:override(Sc, Override)};
            (FieldSchema) ->
                FieldSchema
        end,
        Fields0 ++ Fields2
    ),
    [
        {topic,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(emqx_bridge_kafka, consumer_kafka_topic)
                }
            )},
        {group_id,
            mk(
                binary(),
                #{
                    required => false,
                    desc => ?DESC(group_id)
                }
            )},
        {max_wait_time,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"1s">>,
                    desc => ?DESC("max_wait_time")
                }
            )}
        | Fields
    ];
%%=========================================
%% HTTP API fields: source
%%=========================================
fields(Field) when
    Field == "get_source";
    Field == "post_source";
    Field == "put_source"
->
    emqx_bridge_v2_schema:api_fields(Field, ?SOURCE_TYPE, fields(consumer_source));
%%=========================================
%% Connector fields
%%=========================================
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        connector_config_fields();
%%=========================================
%% HTTP API fields: connector
%%=========================================
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        connector_config_fields()
    ).

desc("config_connector") ->
    ?DESC("config_connector");
desc(source_parameters) ->
    ?DESC(source_parameters);
desc(consumer_source) ->
    ?DESC(consumer_source);
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(source_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(Field) when
    Field =:= "get_connector";
    Field =:= "put_connector";
    Field =:= "post_connector"
->
    "Configuration for Kafka Consumer Connector.";
desc(Field) when
    Field =:= "get_source";
    Field =:= "put_source";
    Field =:= "post_source"
->
    "Configuration for Kafka Consumer Source.";
desc(Name) ->
    throw({missing_desc, ?MODULE, Name}).

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

source_examples(Method) ->
    [
        #{
            <<"kafka_consumer">> => #{
                summary => <<"Kafka Consumer Source">>,
                value => source_example(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"kafka_consumer">> => #{
                summary => <<"Kafka Consumer Connector">>,
                value => connector_example(Method)
            }
        }
    ].

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

source_example(post) ->
    maps:merge(
        source_example(put),
        #{
            type => <<"kafka_consumer">>,
            name => <<"my_source">>
        }
    );
source_example(get) ->
    maps:merge(
        source_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
source_example(put) ->
    #{
        parameters =>
            #{
                topic => <<"mytopic">>
            },
        resource_opts =>
            #{
                health_check_interval => <<"30s">>
            }
    }.

connector_example(get) ->
    maps:merge(
        connector_example(post),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => <<"kafka_consumer">>,
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        bootstrap_hosts => <<"kafka.emqx.net:9092">>,
        resource_opts =>
            #{
                start_after_created => true,
                health_check_interval => <<"30s">>,
                start_timeout => <<"5s">>
            }
    }.

legacy_consumer_topic_mapping_validator(_TopicMapping = []) ->
    %% Can be (and should be, unless it has migrated from v1) empty in v2.
    ok;
legacy_consumer_topic_mapping_validator(TopicMapping = [_ | _]) ->
    emqx_bridge_kafka:consumer_topic_mapping_validator(TopicMapping).

connector_config_fields() ->
    lists:map(
        fun
            ({health_check_topic = Name, Sc}) ->
                %% This field was accidentally added to the consumer schema because it was
                %% added to the shared, v1 field in the shared schema module.
                Override = #{
                    %% to please dialyzer...
                    type => hocon_schema:field_schema(Sc, type),
                    deprecated => {since, "5.9.0"},
                    importance => ?IMPORTANCE_HIDDEN
                },
                {Name, hocon_schema:override(Sc, Override)};
            (FieldSchema) ->
                FieldSchema
        end,
        emqx_bridge_kafka:kafka_connector_config_fields()
    ).
