%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% allow atoms like scram_sha_256 and scram_sha_512
%% i.e. the _256 part does not start with a-z
-elvis([
    {elvis_style, atom_naming_convention, #{
        regex => "^([a-z][a-z0-9]*_?)([a-z0-9]*_?)*$",
        enclosed_atoms => ".*"
    }}
]).
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    host_opts/0
]).

-export([kafka_producer_converter/2]).

%% -------------------------------------------------------------------------------------------------
%% api

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

values({get, KafkaType}) ->
    values({post, KafkaType});
values({post, KafkaType}) ->
    maps:merge(values(common_config), values(KafkaType));
values({put, KafkaType}) ->
    values({post, KafkaType});
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
            nodelay => true
        }
    };
values(producer) ->
    #{
        kafka => #{
            topic => <<"kafka-topic">>,
            message => #{
                key => <<"${.clientid}">>,
                value => <<"${.}">>,
                timestamp => <<"${.timestamp}">>
            },
            max_batch_bytes => <<"896KB">>,
            compression => <<"no_compression">>,
            partition_strategy => <<"random">>,
            required_acks => <<"all_isr">>,
            partition_count_refresh_interval => <<"60s">>,
            max_inflight => 10,
            buffer => #{
                mode => <<"hybrid">>,
                per_partition_limit => <<"2GB">>,
                segment_bytes => <<"100MB">>,
                memory_overload_protection => true
            }
        },
        local_topic => <<"mqtt/local/topic">>
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
                mqtt_topic => <<"mqtt/topic/1">>,
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

host_opts() ->
    #{default_port => 9092}.

namespace() -> "bridge_kafka".

roots() -> ["config_consumer", "config_producer"].

fields("post_" ++ Type) ->
    [type_field(), name_field() | fields("config_" ++ Type)];
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("config_producer") ->
    fields(kafka_producer);
fields("config_consumer") ->
    fields(kafka_consumer);
fields(kafka_producer) ->
    fields("config") ++ fields(producer_opts);
fields(kafka_consumer) ->
    fields("config") ++ fields(consumer_opts);
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
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
            mk(emqx_schema:duration_ms(), #{
                default => <<"5s">>,
                desc => ?DESC(connect_timeout)
            })},
        {min_metadata_refresh_interval,
            mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"3s">>,
                    desc => ?DESC(min_metadata_refresh_interval)
                }
            )},
        {metadata_request_timeout,
            mk(emqx_schema:duration_ms(), #{
                default => <<"5s">>,
                desc => ?DESC(metadata_request_timeout)
            })},
        {authentication,
            mk(hoconsc:union([none, ref(auth_username_password), ref(auth_gssapi_kerberos)]), #{
                default => none, desc => ?DESC("authentication")
            })},
        {socket_opts, mk(ref(socket_opts), #{required => false, desc => ?DESC(socket_opts)})}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields(auth_username_password) ->
    [
        {mechanism,
            mk(enum([plain, scram_sha_256, scram_sha_512]), #{
                required => true, desc => ?DESC(auth_sasl_mechanism)
            })},
        {username, mk(binary(), #{required => true, desc => ?DESC(auth_sasl_username)})},
        {password,
            mk(binary(), #{
                required => true,
                sensitive => true,
                desc => ?DESC(auth_sasl_password),
                converter => fun emqx_schema:password_converter/2
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
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(socket_nodelay)
                }
            )}
    ];
fields(producer_opts) ->
    [
        %% Note: there's an implicit convention in `emqx_bridge' that,
        %% for egress bridges with this config, the published messages
        %% will be forwarded to such bridges.
        {local_topic, mk(binary(), #{required => false, desc => ?DESC(mqtt_topic)})},
        {kafka,
            mk(ref(producer_kafka_opts), #{
                required => true,
                desc => ?DESC(producer_kafka_opts)
            })}
    ];
fields(producer_kafka_opts) ->
    [
        {topic, mk(string(), #{required => true, desc => ?DESC(kafka_topic)})},
        {message, mk(ref(kafka_message), #{required => false, desc => ?DESC(kafka_message)})},
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
        {partition_count_refresh_interval,
            mk(
                emqx_schema:duration_s(),
                #{
                    default => <<"60s">>,
                    desc => ?DESC(partition_count_refresh_interval)
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
            })}
    ];
fields(kafka_message) ->
    [
        {key, mk(string(), #{default => <<"${.clientid}">>, desc => ?DESC(kafka_message_key)})},
        {value, mk(string(), #{default => <<"${.}">>, desc => ?DESC(kafka_message_value)})},
        {timestamp,
            mk(string(), #{
                default => <<"${.timestamp}">>, desc => ?DESC(kafka_message_timestamp)
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
                #{default => <<"100MB">>, desc => ?DESC(buffer_segment_bytes)}
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
                string(),
                #{default => <<"${.}">>, desc => ?DESC(consumer_mqtt_payload)}
            )}
    ];
fields(consumer_kafka_opts) ->
    [
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{
                default => "896KB", desc => ?DESC(consumer_max_batch_bytes)
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
                pos_integer(),
                #{default => 5, desc => ?DESC(consumer_offset_commit_interval_seconds)}
            )}
    ].

desc("config") ->
    ?DESC("desc_config");
desc("get_" ++ Type) when Type =:= "consumer"; Type =:= "producer" ->
    ["Configuration for Kafka using `GET` method."];
desc("put_" ++ Type) when Type =:= "consumer"; Type =:= "producer" ->
    ["Configuration for Kafka using `PUT` method."];
desc("post_" ++ Type) when Type =:= "consumer"; Type =:= "producer" ->
    ["Configuration for Kafka using `POST` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

struct_names() ->
    [
        auth_gssapi_kerberos,
        auth_username_password,
        kafka_message,
        kafka_producer,
        kafka_consumer,
        producer_buffer,
        producer_kafka_opts,
        socket_opts,
        producer_opts,
        consumer_opts,
        consumer_kafka_opts,
        consumer_topic_mapping
    ].

%% -------------------------------------------------------------------------------------------------
%% internal
type_field() ->
    {type,
        mk(enum([kafka_consumer, kafka_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

ref(Name) ->
    hoconsc:ref(?MODULE, Name).

kafka_producer_converter(undefined, _HoconOpts) ->
    undefined;
kafka_producer_converter(
    #{<<"producer">> := OldOpts0, <<"bootstrap_hosts">> := _} = Config0, _HoconOpts
) ->
    %% old schema
    MQTTOpts = maps:get(<<"mqtt">>, OldOpts0, #{}),
    LocalTopic = maps:get(<<"topic">>, MQTTOpts, undefined),
    KafkaOpts = maps:get(<<"kafka">>, OldOpts0),
    Config = maps:without([<<"producer">>], Config0),
    case LocalTopic =:= undefined of
        true ->
            Config#{<<"kafka">> => KafkaOpts};
        false ->
            Config#{<<"kafka">> => KafkaOpts, <<"local_topic">> => LocalTopic}
    end;
kafka_producer_converter(Config, _HoconOpts) ->
    %% new schema
    Config.

consumer_topic_mapping_validator(_TopicMapping = []) ->
    {error, "There must be at least one Kafka-MQTT topic mapping"};
consumer_topic_mapping_validator(TopicMapping = [_ | _]) ->
    NumEntries = length(TopicMapping),
    KafkaTopics = [KT || #{<<"kafka_topic">> := KT} <- TopicMapping],
    DistinctKafkaTopics = length(lists:usort(KafkaTopics)),
    case DistinctKafkaTopics =:= NumEntries of
        true ->
            ok;
        false ->
            {error, "Kafka topics must not be repeated in a bridge"}
    end.
