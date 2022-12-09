%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"kafka">> => #{
                summary => <<"Kafka Bridge">>,
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

namespace() -> "bridge_kafka".

roots() -> ["config"].

fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post");
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {bootstrap_hosts, mk(binary(), #{required => true, desc => ?DESC(bootstrap_hosts)})},
        {connect_timeout,
            mk(emqx_schema:duration_ms(), #{
                default => "5s",
                desc => ?DESC(connect_timeout)
            })},
        {min_metadata_refresh_interval,
            mk(
                emqx_schema:duration_ms(),
                #{
                    default => "3s",
                    desc => ?DESC(min_metadata_refresh_interval)
                }
            )},
        {metadata_request_timeout,
            mk(emqx_schema:duration_ms(), #{
                default => "5s",
                desc => ?DESC(metadata_request_timeout)
            })},
        {authentication,
            mk(hoconsc:union([none, ref(auth_username_password), ref(auth_gssapi_kerberos)]), #{
                default => none, desc => ?DESC("authentication")
            })},
        {producer, mk(hoconsc:union([none, ref(producer_opts)]), #{desc => ?DESC(producer_opts)})},
        %{consumer, mk(hoconsc:union([none, ref(consumer_opts)]), #{desc => ?DESC(consumer_opts)})},
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
            mk(binary(), #{required => true, sensitive => true, desc => ?DESC(auth_sasl_password)})}
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
                #{default => "1024KB", desc => ?DESC(socket_send_buffer)}
            )},
        {recbuf,
            mk(
                emqx_schema:bytesize(),
                #{default => "1024KB", desc => ?DESC(socket_receive_buffer)}
            )},
        {nodelay,
            mk(
                boolean(),
                #{default => true, desc => ?DESC(socket_nodelay)}
            )}
    ];
fields(producer_opts) ->
    [
        {mqtt, mk(ref(producer_mqtt_opts), #{desc => ?DESC(producer_mqtt_opts)})},
        {kafka,
            mk(ref(producer_kafka_opts), #{
                required => true,
                desc => ?DESC(producer_kafka_opts)
            })}
    ];
fields(producer_mqtt_opts) ->
    [{topic, mk(binary(), #{desc => ?DESC(mqtt_topic)})}];
fields(producer_kafka_opts) ->
    [
        {topic, mk(string(), #{required => true, desc => ?DESC(kafka_topic)})},
        {message, mk(ref(kafka_message), #{required => false, desc => ?DESC(kafka_message)})},
        {max_batch_bytes,
            mk(emqx_schema:bytesize(), #{default => "896KB", desc => ?DESC(max_batch_bytes)})},
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
                    default => "60s",
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
        {key, mk(string(), #{default => "${clientid}", desc => ?DESC(kafka_message_key)})},
        {value, mk(string(), #{default => "${payload}", desc => ?DESC(kafka_message_value)})},
        {timestamp,
            mk(string(), #{
                default => "${timestamp}", desc => ?DESC(kafka_message_timestamp)
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
                #{default => "2GB", desc => ?DESC(buffer_per_partition_limit)}
            )},
        {segment_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => "100MB", desc => ?DESC(buffer_segment_bytes)}
            )},
        {memory_overload_protection,
            mk(boolean(), #{
                %% different from 4.x
                default => true,
                desc => ?DESC(buffer_memory_overload_protection)
            })}
    ].

% fields(consumer_opts) ->
%     [
%         {kafka, mk(ref(consumer_kafka_opts), #{required => true, desc => ?DESC(consumer_kafka_opts)})},
%         {mqtt, mk(ref(consumer_mqtt_opts), #{required => true, desc => ?DESC(consumer_mqtt_opts)})}
%     ];
% fields(consumer_mqtt_opts) ->
%     [ {topic, mk(string(), #{desc => ?DESC(consumer_mqtt_topic)})}
%     ];

% fields(consumer_mqtt_opts) ->
%     [ {topic, mk(string(), #{desc => ?DESC(consumer_mqtt_topic)})}
%     ];
% fields(consumer_kafka_opts) ->
%     [ {topic, mk(string(), #{desc => ?DESC(consumer_kafka_topic)})}
%     ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Kafka using `", string:to_upper(Method), "` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

struct_names() ->
    [
        auth_gssapi_kerberos,
        auth_username_password,
        kafka_message,
        producer_buffer,
        producer_kafka_opts,
        producer_mqtt_opts,
        socket_opts,
        producer_opts
    ].

%% -------------------------------------------------------------------------------------------------
%% internal
type_field() ->
    {type, mk(enum([kafka]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

ref(Name) ->
    hoconsc:ref(?MODULE, Name).
