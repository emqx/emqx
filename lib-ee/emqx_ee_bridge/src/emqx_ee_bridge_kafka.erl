%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_kafka).

-include_lib("emqx_bridge/include/emqx_bridge.hrl").
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
            %% TODO: rename this to `kafka_producer' after alias
            %% support is added to hocon; keeping this as just `kafka'
            %% for backwards compatibility.
            <<"kafka">> => #{
                summary => <<"Kafka Producer Bridge">>,
                value => values(Method)
            }
        },
        #{
            <<"kafka_consumer">> => #{
                summary => <<"Kafka Consumer Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(values(post), ?METRICS_EXAMPLE);
values(post) ->
    #{
        bootstrap_hosts => <<"localhost:9092">>
    };
values(put) ->
    values(post).

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
                #{default => <<"1024KB">>, desc => ?DESC(socket_send_buffer)}
            )},
        {recbuf,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1024KB">>, desc => ?DESC(socket_receive_buffer)}
            )},
        {nodelay,
            mk(
                boolean(),
                #{default => true, desc => ?DESC(socket_nodelay)}
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
            mk(ref(consumer_kafka_opts), #{required => true, desc => ?DESC(consumer_kafka_opts)})},
        {mqtt, mk(ref(consumer_mqtt_opts), #{required => true, desc => ?DESC(consumer_mqtt_opts)})}
    ];
fields(consumer_mqtt_opts) ->
    [
        {topic,
            mk(binary(), #{
                required => true,
                desc => ?DESC(consumer_mqtt_topic)
            })},
        {qos, mk(emqx_schema:qos(), #{default => 0, desc => ?DESC(consumer_mqtt_qos)})},
        {payload,
            mk(
                enum([full_message, message_value]),
                #{default => full_message, desc => ?DESC(consumer_mqtt_payload)}
            )}
    ];
fields(consumer_kafka_opts) ->
    [
        {topic, mk(binary(), #{desc => ?DESC(consumer_kafka_topic)})},
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{
                default => "896KB", desc => ?DESC(consumer_max_batch_bytes)
            })},
        {max_rejoin_attempts,
            mk(non_neg_integer(), #{
                hidden => true,
                default => 5,
                desc => ?DESC(consumer_max_rejoin_attempts)
            })},
        {offset_reset_policy,
            mk(
                enum([reset_to_latest, reset_to_earliest, reset_by_subscriber]),
                #{default => reset_to_latest, desc => ?DESC(consumer_offset_reset_policy)}
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
        consumer_mqtt_opts
    ].

%% -------------------------------------------------------------------------------------------------
%% internal
type_field() ->
    {type,
        %% TODO: rename `kafka' to `kafka_producer' after alias
        %% support is added to hocon; keeping this as just `kafka' for
        %% backwards compatibility.
        mk(enum([kafka_consumer, kafka]), #{required => true, desc => ?DESC("desc_type")})}.

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
