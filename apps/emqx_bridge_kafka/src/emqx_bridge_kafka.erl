%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka).

-feature(maybe_expr, enable).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    host_opts/0,
    ssl_client_opts_fields/0,
    producer_opts/1
]).

%% Internal export to be used in v2 schema
-export([consumer_topic_mapping_validator/1]).

-export([
    kafka_connector_config_fields/0,
    kafka_producer_converter/2,
    producer_strategy_key_validator/1,
    producer_buffer_mode_validator/1,
    producer_parameters_validator/1
]).

-define(CONNECTOR_TYPE, kafka_producer).

%% -------------------------------------------------------------------------------------------------
%% api

connector_examples(Method) ->
    [
        #{
            <<"kafka_producer">> => #{
                summary => <<"Kafka Producer Connector">>,
                value => values({Method, connector})
            }
        }
    ].

bridge_v2_examples(Method) ->
    [
        #{
            <<"kafka_producer">> => #{
                summary => <<"Kafka Producer Action">>,
                value => values({Method, bridge_v2_producer})
            }
        }
    ].

conn_bridge_examples(Method) ->
    [
        #{
            <<"kafka_producer">> => #{
                summary => <<"Kafka Producer Bridge">>,
                value => values({Method, producer})
            }
        },
        #{
            <<"kafka_consumer">> => #{
                summary => <<"Kafka Consumer Bridge">>,
                value => values({Method, consumer})
            }
        }
    ].

values({get, connector}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values({post, connector})
    );
values({get, KafkaType}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values({post, KafkaType})
    );
values({post, connector}) ->
    maps:merge(
        #{
            name => <<"my_kafka_producer_connector">>,
            type => <<"kafka_producer">>
        },
        values(common_config)
    );
values({post, KafkaType}) ->
    maps:merge(
        #{
            name => <<"my_kafka_producer_action">>,
            type => <<"kafka_producer">>
        },
        values({put, KafkaType})
    );
values({put, bridge_v2_producer}) ->
    values(bridge_v2_producer);
values({put, connector}) ->
    values(common_config);
values({put, KafkaType}) ->
    maps:merge(values(common_config), values(KafkaType));
values(bridge_v2_producer) ->
    #{
        enable => true,
        connector => <<"my_kafka_producer_connector">>,
        parameters => values(producer_values),
        local_topic => <<"mqtt/local/topic">>,
        resource_opts => #{
            health_check_interval => "32s"
        }
    };
values(common_config) ->
    #{
        authentication => #{
            mechanism => <<"plain">>,
            username => <<"username">>,
            password => <<"******">>
        },
        bootstrap_hosts => <<"localhost:9092">>,
        connect_timeout => <<"5s">>,
        enable => true,
        metadata_request_timeout => <<"4s">>,
        min_metadata_refresh_interval => <<"3s">>,
        socket_opts => #{
            sndbuf => <<"1024KB">>,
            recbuf => <<"1024KB">>,
            nodelay => true,
            tcp_keepalive => <<"none">>
        }
    };
values(producer) ->
    #{
        kafka => values(producer_values),
        local_topic => <<"mqtt/local/topic">>
    };
values(producer_values) ->
    #{
        topic => <<"kafka-topic">>,
        message => #{
            key => <<"${.clientid}">>,
            value => <<"${.}">>,
            timestamp => <<"${.timestamp}">>
        },
        max_linger_time => <<"5ms">>,
        max_linger_bytes => <<"10MB">>,
        max_batch_bytes => <<"896KB">>,
        compression => <<"no_compression">>,
        partition_strategy => <<"random">>,
        required_acks => <<"all_isr">>,
        partition_count_refresh_interval => <<"60s">>,
        kafka_headers => <<"${pub_props}">>,
        kafka_ext_headers => [
            #{
                kafka_ext_header_key => <<"clientid">>,
                kafka_ext_header_value => <<"${clientid}">>
            },
            #{
                kafka_ext_header_key => <<"topic">>,
                kafka_ext_header_value => <<"${topic}">>
            }
        ],
        kafka_header_value_encode_mode => none,
        max_inflight => 10,
        partitions_limit => all_partitions,
        buffer => #{
            mode => <<"hybrid">>,
            per_partition_limit => <<"2GB">>,
            segment_bytes => <<"10MB">>,
            memory_overload_protection => true
        }
    };
values(consumer) ->
    #{
        kafka => #{
            max_batch_bytes => <<"896KB">>,
            offset_reset_policy => <<"latest">>,
            offset_commit_interval_seconds => 5
        },
        key_encoding_mode => <<"none">>,
        topic_mapping => [
            #{
                kafka_topic => <<"kafka-topic-1">>,
                mqtt_topic => <<"mqtt/topic/${.offset}">>,
                qos => 1,
                payload_template => <<"${.}">>
            },
            #{
                kafka_topic => <<"kafka-topic-2">>,
                mqtt_topic => <<"mqtt/topic/2">>,
                qos => 2,
                payload_template => <<"v = ${.value}">>
            }
        ],
        value_encoding_mode => <<"none">>
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

%% In addition to the common ssl client options defined in emqx_schema module
%% Kafka supports a special value 'auto' in order to support different bootstrap endpoints
%% as well as partition leaders.
%% A static SNI is quite unusual for Kafka, but it's kept anyway.
ssl_overrides() ->
    #{
        "server_name_indication" =>
            mk(
                hoconsc:union([auto, disable, string()]),
                #{
                    example => auto,
                    default => <<"auto">>,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC("server_name_indication")
                }
            )
    }.

override(Fields, Overrides) ->
    lists:map(
        fun({Name, Sc}) ->
            case maps:find(Name, Overrides) of
                {ok, Override} ->
                    {Name, hocon_schema:override(Sc, Override)};
                error ->
                    {Name, Sc}
            end
        end,
        Fields
    ).

ssl_client_opts_fields() ->
    override(emqx_schema:client_ssl_opts_schema(#{}), ssl_overrides()).

host_opts() ->
    #{default_port => 9092}.

namespace() -> "bridge_kafka".

roots() -> ["config_consumer", "config_producer", "config_bridge_v2"].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        kafka_connector_config_fields()
    );
fields("post_" ++ Type) ->
    [type_field(Type), name_field() | fields("config_" ++ Type)];
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("config_bridge_v2") ->
    fields(kafka_producer_action);
fields("config_connector") ->
    connector_config_fields();
fields("config_producer") ->
    fields(kafka_producer);
fields("config_consumer") ->
    fields(kafka_consumer);
fields(kafka_producer) ->
    %% Schema used by bridges V1.
    connector_config_fields() ++ producer_opts(v1);
fields(kafka_producer_action) ->
    emqx_bridge_v2_schema:common_fields() ++ producer_opts(action);
fields(kafka_consumer) ->
    connector_config_fields() ++ fields(consumer_opts);
fields(ssl_client_opts) ->
    ssl_client_opts_fields();
fields(auth_username_password) ->
    [
        {mechanism,
            mk(enum([plain, scram_sha_256, scram_sha_512]), #{
                required => true, desc => ?DESC(auth_sasl_mechanism)
            })},
        {username, mk(binary(), #{required => true, desc => ?DESC(auth_sasl_username)})},
        {password,
            emqx_connector_schema_lib:password_field(#{
                required => true,
                desc => ?DESC(auth_sasl_password)
            })}
    ];
fields(auth_gssapi_kerberos) ->
    [
        {kerberos_principal,
            mk(binary(), #{
                required => true,
                desc => ?DESC(auth_kerberos_principal)
            })},
        {kerberos_keytab_file,
            mk(binary(), #{
                required => true,
                desc => ?DESC(auth_kerberos_keytab_file)
            })}
    ];
fields(socket_opts) ->
    [
        {sndbuf,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1MB">>, desc => ?DESC(socket_send_buffer)}
            )},
        {recbuf,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1MB">>, desc => ?DESC(socket_receive_buffer)}
            )},
        {nodelay,
            mk(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(socket_nodelay)
                }
            )},
        {tcp_keepalive,
            mk(string(), #{
                default => <<"none">>,
                desc => ?DESC(emqx_schema, socket_tcp_keepalive),
                validator => fun emqx_schema:validate_tcp_keepalive/1
            })}
    ];
fields(v1_producer_kafka_opts) ->
    OldSchemaFields =
        [
            topic,
            message,
            max_batch_bytes,
            compression,
            partition_strategy,
            required_acks,
            kafka_headers,
            kafka_ext_headers,
            kafka_header_value_encode_mode,
            partition_count_refresh_interval,
            partitions_limit,
            max_inflight,
            buffer,
            query_mode,
            sync_query_timeout
        ],
    Fields = fields(producer_kafka_opts),
    lists:filter(
        fun({K, _V}) -> lists:member(K, OldSchemaFields) end,
        Fields
    );
fields(producer_kafka_opts) ->
    [
        {topic, mk(emqx_schema:template(), #{required => true, desc => ?DESC(kafka_topic)})},
        {message, mk(ref(kafka_message), #{required => false, desc => ?DESC(kafka_message)})},
        {max_linger_time,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"0ms">>,
                desc => ?DESC(max_linger_time)
            })},
        {max_linger_bytes,
            mk(emqx_schema:bytesize(), #{
                default => <<"10MB">>,
                desc => ?DESC(max_linger_bytes)
            })},
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{default => <<"896KB">>, desc => ?DESC(max_batch_bytes)})},
        {compression,
            mk(enum([no_compression, snappy, gzip]), #{
                default => no_compression, desc => ?DESC(compression)
            })},
        {partition_strategy,
            mk(
                enum([random, key_dispatch]),
                #{default => random, desc => ?DESC(partition_strategy)}
            )},
        {required_acks,
            mk(
                enum([all_isr, leader_only, none]),
                #{
                    default => all_isr,
                    desc => ?DESC(required_acks)
                }
            )},
        {kafka_headers,
            mk(
                emqx_schema:template(),
                #{
                    required => false,
                    validator => fun kafka_header_validator/1,
                    desc => ?DESC(kafka_headers)
                }
            )},
        {kafka_ext_headers,
            mk(
                hoconsc:array(ref(producer_kafka_ext_headers)),
                #{
                    desc => ?DESC(producer_kafka_ext_headers),
                    required => false
                }
            )},
        {kafka_header_value_encode_mode,
            mk(
                enum([none, json]),
                #{
                    default => none,
                    desc => ?DESC(kafka_header_value_encode_mode)
                }
            )},
        {partition_count_refresh_interval,
            mk(
                emqx_schema:timeout_duration_s(),
                #{
                    default => <<"60s">>,
                    desc => ?DESC(partition_count_refresh_interval)
                }
            )},
        {partitions_limit,
            mk(
                hoconsc:union([all_partitions, pos_integer()]),
                #{
                    default => <<"all_partitions">>,
                    desc => ?DESC(partitions_limit)
                }
            )},
        {max_inflight,
            mk(
                pos_integer(),
                #{
                    default => 10,
                    desc => ?DESC(max_inflight)
                }
            )},
        {buffer,
            mk(ref(producer_buffer), #{
                required => false,
                desc => ?DESC(producer_buffer)
            })},
        {query_mode,
            mk(
                enum([async, sync]),
                #{
                    default => async,
                    desc => ?DESC(query_mode)
                }
            )},
        {sync_query_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"5s">>,
                    desc => ?DESC(sync_query_timeout)
                }
            )}
    ];
fields(producer_kafka_ext_headers) ->
    [
        {kafka_ext_header_key,
            mk(
                emqx_schema:template(),
                #{required => true, desc => ?DESC(producer_kafka_ext_header_key)}
            )},
        {kafka_ext_header_value,
            mk(
                emqx_schema:template(),
                #{
                    required => true,
                    validator => fun kafka_ext_header_value_validator/1,
                    desc => ?DESC(producer_kafka_ext_header_value)
                }
            )}
    ];
fields(kafka_message) ->
    [
        {key,
            mk(emqx_schema:template(), #{
                default => <<"${.clientid}">>,
                desc => ?DESC(kafka_message_key)
            })},
        {value,
            mk(emqx_schema:template(), #{
                default => <<"${.}">>,
                desc => ?DESC(kafka_message_value)
            })},
        {timestamp,
            mk(emqx_schema:template(), #{
                default => <<"${.timestamp}">>,
                desc => ?DESC(kafka_message_timestamp)
            })}
    ];
fields(producer_buffer) ->
    [
        {mode,
            mk(
                enum([memory, disk, hybrid]),
                #{default => memory, desc => ?DESC(buffer_mode)}
            )},
        {per_partition_limit,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"2GB">>, desc => ?DESC(buffer_per_partition_limit)}
            )},
        {segment_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"10MB">>, desc => ?DESC(buffer_segment_bytes)}
            )},
        {memory_overload_protection,
            mk(boolean(), #{
                default => false,
                desc => ?DESC(buffer_memory_overload_protection)
            })}
    ];
fields(consumer_opts) ->
    [
        {kafka,
            mk(ref(consumer_kafka_opts), #{required => false, desc => ?DESC(consumer_kafka_opts)})},
        {topic_mapping,
            mk(
                hoconsc:array(ref(consumer_topic_mapping)),
                #{
                    required => true,
                    desc => ?DESC(consumer_topic_mapping),
                    validator => fun consumer_topic_mapping_validator/1
                }
            )},
        {key_encoding_mode,
            mk(enum([none, base64]), #{
                default => none, desc => ?DESC(consumer_key_encoding_mode)
            })},
        {value_encoding_mode,
            mk(enum([none, base64]), #{
                default => none, desc => ?DESC(consumer_value_encoding_mode)
            })}
    ];
fields(consumer_topic_mapping) ->
    [
        {kafka_topic, mk(binary(), #{required => true, desc => ?DESC(consumer_kafka_topic)})},
        {mqtt_topic, mk(binary(), #{required => true, desc => ?DESC(consumer_mqtt_topic)})},
        {qos, mk(emqx_schema:qos(), #{default => 0, desc => ?DESC(consumer_mqtt_qos)})},
        {payload_template,
            mk(
                emqx_schema:template(),
                #{
                    default => <<"${.}">>,
                    desc => ?DESC(consumer_mqtt_payload)
                }
            )}
    ];
fields(consumer_kafka_opts) ->
    [
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{
                default => <<"896KB">>, desc => ?DESC(consumer_max_batch_bytes)
            })},
        {max_rejoin_attempts,
            mk(non_neg_integer(), #{
                importance => ?IMPORTANCE_HIDDEN,
                default => 5,
                desc => ?DESC(consumer_max_rejoin_attempts)
            })},
        {offset_reset_policy,
            mk(
                enum([latest, earliest]),
                #{default => latest, desc => ?DESC(consumer_offset_reset_policy)}
            )},
        {offset_commit_interval_seconds,
            mk(
                emqx_schema:timeout_duration_s(),
                #{default => <<"5s">>, desc => ?DESC(consumer_offset_commit_interval_seconds)}
            )}
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(resource_opts) ->
    SupportedFields = [health_check_interval],
    CreationOpts = emqx_bridge_v2_schema:action_resource_opts_fields(),
    lists:filter(fun({Field, _}) -> lists:member(Field, SupportedFields) end, CreationOpts);
fields(action_field) ->
    {kafka_producer,
        mk(
            hoconsc:map(name, ref(emqx_bridge_kafka, kafka_producer_action)),
            #{
                desc => <<"Kafka Producer Action Config">>,
                required => false
            }
        )};
fields(action) ->
    fields(action_field).

desc("config_connector") ->
    ?DESC("desc_config");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc("get_" ++ Type) when
    Type =:= "consumer"; Type =:= "producer"; Type =:= "connector"; Type =:= "bridge_v2"
->
    ["Configuration for Kafka using `GET` method."];
desc("put_" ++ Type) when
    Type =:= "consumer"; Type =:= "producer"; Type =:= "connector"; Type =:= "bridge_v2"
->
    ["Configuration for Kafka using `PUT` method."];
desc("post_" ++ Type) when
    Type =:= "consumer"; Type =:= "producer"; Type =:= "connector"; Type =:= "bridge_v2"
->
    ["Configuration for Kafka using `POST` method."];
desc(kafka_producer_action) ->
    ?DESC("kafka_producer_action");
desc(Name) ->
    ?DESC(Name).

connector_config_fields() ->
    emqx_connector_schema:common_fields() ++
        kafka_connector_config_fields().

kafka_connector_config_fields() ->
    [
        {bootstrap_hosts,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(bootstrap_hosts),
                    validator => emqx_schema:servers_validator(
                        host_opts(), _Required = true
                    )
                }
            )},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"5s">>,
                desc => ?DESC(connect_timeout)
            })},
        {min_metadata_refresh_interval,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"3s">>,
                    desc => ?DESC(min_metadata_refresh_interval)
                }
            )},
        {metadata_request_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"5s">>,
                desc => ?DESC(metadata_request_timeout)
            })},
        {authentication,
            mk(hoconsc:union([none, ref(auth_username_password), ref(auth_gssapi_kerberos)]), #{
                default => none, desc => ?DESC("authentication")
            })},
        {socket_opts, mk(ref(socket_opts), #{required => false, desc => ?DESC(socket_opts)})},
        {health_check_topic,
            mk(binary(), #{
                required => false,
                desc => ?DESC(producer_health_check_topic)
            })},
        {ssl, mk(ref(ssl_client_opts), #{})}
    ] ++ emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts).

producer_opts(ActionOrBridgeV1) ->
    [
        %% Note: there's an implicit convention in `emqx_bridge' that,
        %% for egress bridges with this config, the published messages
        %% will be forwarded to such bridges.
        {local_topic, mk(binary(), #{required => false, desc => ?DESC(mqtt_topic)})},
        parameters_field(ActionOrBridgeV1)
    ] ++ [resource_opts() || ActionOrBridgeV1 =:= action].

resource_opts() ->
    {resource_opts, mk(ref(resource_opts), #{default => #{}, desc => ?DESC(resource_opts)})}.

%% Since e5.3.1, we want to rename the field 'kafka' to 'parameters'
%% However we need to keep it backward compatible for generated schema json (version 0.1.0)
%% since schema is data for the 'schemas' API.
parameters_field(ActionOrBridgeV1) ->
    {Name, Alias, Ref} =
        case ActionOrBridgeV1 of
            v1 ->
                {kafka, parameters, v1_producer_kafka_opts};
            action ->
                {parameters, kafka, producer_kafka_opts}
        end,
    {Name,
        mk(ref(Ref), #{
            required => true,
            aliases => [Alias],
            desc => ?DESC(producer_kafka_opts),
            validator => fun producer_parameters_validator/1
        })}.

%% -------------------------------------------------------------------------------------------------
%% internal
type_field(BridgeV2Type) when BridgeV2Type =:= "connector"; BridgeV2Type =:= "bridge_v2" ->
    {type, mk(enum([kafka_producer]), #{required => true, desc => ?DESC("desc_type")})};
type_field(_) ->
    {type,
        %% 'kafka' is kept for backward compatibility
        mk(enum([kafka, kafka_producer, kafka_consumer]), #{
            required => true, desc => ?DESC("desc_type")
        })}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

ref(Name) ->
    hoconsc:ref(?MODULE, Name).

kafka_producer_converter(undefined, _HoconOpts) ->
    undefined;
kafka_producer_converter(
    #{<<"producer">> := OldOpts0, <<"bootstrap_hosts">> := _} = Config0, _HoconOpts
) ->
    %% prior to e5.0.2
    MQTTOpts = maps:get(<<"mqtt">>, OldOpts0, #{}),
    LocalTopic = maps:get(<<"topic">>, MQTTOpts, undefined),
    KafkaOpts = maps:get(<<"kafka">>, OldOpts0),
    Config = maps:without([<<"producer">>], Config0),
    case LocalTopic =:= undefined of
        true ->
            Config#{<<"parameters">> => KafkaOpts};
        false ->
            Config#{<<"parameters">> => KafkaOpts, <<"local_topic">> => LocalTopic}
    end;
kafka_producer_converter(
    #{<<"kafka">> := _} = Config0, _HoconOpts
) ->
    %% from e5.0.2 to e5.3.0
    {KafkaOpts, Config} = maps:take(<<"kafka">>, Config0),
    Config#{<<"parameters">> => KafkaOpts};
kafka_producer_converter(Config, _HoconOpts) ->
    %% new schema
    Config.

consumer_topic_mapping_validator(_TopicMapping = []) ->
    {error, "There must be at least one Kafka-MQTT topic mapping"};
consumer_topic_mapping_validator(TopicMapping0 = [_ | _]) ->
    TopicMapping = [emqx_utils_maps:binary_key_map(TM) || TM <- TopicMapping0],
    NumEntries = length(TopicMapping),
    KafkaTopics = [KT || #{<<"kafka_topic">> := KT} <- TopicMapping],
    DistinctKafkaTopics = length(lists:usort(KafkaTopics)),
    case DistinctKafkaTopics =:= NumEntries of
        true ->
            ok;
        false ->
            {error, "Kafka topics must not be repeated in a bridge"}
    end.

producer_parameters_validator(Conf) ->
    maybe
        ok ?= producer_strategy_key_validator(Conf),
        ok ?= producer_buffer_mode_validator(Conf)
    end.

producer_buffer_mode_validator(#{buffer := _} = Conf) ->
    producer_buffer_mode_validator(emqx_utils_maps:binary_key_map(Conf));
producer_buffer_mode_validator(#{<<"buffer">> := #{<<"mode">> := disk}, <<"topic">> := Topic}) ->
    Template = emqx_template:parse(Topic),
    case emqx_template:placeholders(Template) of
        [] ->
            ok;
        [_ | _] ->
            {error, <<"disk-mode buffering is disallowed when using dynamic topics">>}
    end;
producer_buffer_mode_validator(_) ->
    %% `buffer' field is not required
    ok.

producer_strategy_key_validator(
    #{
        partition_strategy := _,
        message := #{key := _}
    } = Conf
) ->
    producer_strategy_key_validator(emqx_utils_maps:binary_key_map(Conf));
producer_strategy_key_validator(#{
    <<"partition_strategy">> := key_dispatch,
    <<"message">> := #{<<"key">> := Key}
}) when Key =:= "" orelse Key =:= <<>> ->
    {error, "Message key cannot be empty when `key_dispatch` strategy is used"};
producer_strategy_key_validator(_) ->
    ok.

kafka_header_validator(undefined) ->
    ok;
kafka_header_validator(Value) ->
    case emqx_placeholder:preproc_tmpl(Value) of
        [{var, _}] ->
            ok;
        _ ->
            {error, "The 'kafka_headers' must be a single placeholder like ${pub_props}"}
    end.

kafka_ext_header_value_validator(undefined) ->
    ok;
kafka_ext_header_value_validator(Value) ->
    case emqx_placeholder:preproc_tmpl(Value) of
        [{Type, _}] when Type =:= var orelse Type =:= str ->
            ok;
        _ ->
            {
                error,
                "The value of 'kafka_ext_headers' must either be a single "
                "placeholder like ${foo}, or a simple string."
            }
    end.
