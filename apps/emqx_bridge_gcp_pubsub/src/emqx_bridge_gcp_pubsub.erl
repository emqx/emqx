%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1]).

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).
-export([
    service_account_json_validator/1,
    service_account_json_converter/2
]).

-export([upgrade_raw_conf/1]).

%% emqx_bridge_enterprise "unofficial" API
-export([conn_bridge_examples/1]).

-define(DEFAULT_PIPELINE_SIZE, 100).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_gcp_pubsub".

roots() ->
    [].

fields("config_producer") ->
    emqx_bridge_schema:common_bridge_fields() ++
        emqx_resource_schema:fields("resource_opts") ++
        fields(connector_config) ++ fields(producer);
fields("config_consumer") ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {resource_opts,
                mk(
                    ref("consumer_resource_opts"),
                    #{required => true, desc => ?DESC(emqx_resource_schema, "creation_opts")}
                )}
        ] ++
        fields(connector_config) ++
        [{consumer, mk(ref(consumer), #{required => true, desc => ?DESC(consumer_opts)})}];
fields(connector_config) ->
    [
        {connect_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("connect_timeout")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {pipelining,
            sc(
                pos_integer(),
                #{
                    default => ?DEFAULT_PIPELINE_SIZE,
                    desc => ?DESC("pipelining")
                }
            )},
        {max_retries,
            sc(
                non_neg_integer(),
                #{
                    required => false,
                    default => 2,
                    desc => ?DESC("max_retries")
                }
            )},
        {request_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    required => false,
                    deprecated => {since, "e5.0.1"},
                    default => <<"15s">>,
                    desc => ?DESC("request_timeout")
                }
            )},
        {service_account_json,
            sc(
                binary(),
                #{
                    required => true,
                    validator => fun ?MODULE:service_account_json_validator/1,
                    converter => fun ?MODULE:service_account_json_converter/2,
                    sensitive => true,
                    desc => ?DESC("service_account_json")
                }
            )}
    ];
fields(producer) ->
    [
        {attributes_template,
            sc(
                hoconsc:array(ref(key_value_pair)),
                #{
                    default => [],
                    desc => ?DESC("attributes_template")
                }
            )},
        {ordering_key_template,
            sc(
                emqx_schema:template(),
                #{
                    default => <<>>,
                    desc => ?DESC("ordering_key_template")
                }
            )},
        {payload_template,
            sc(
                emqx_schema:template(),
                #{
                    default => <<>>,
                    desc => ?DESC("payload_template")
                }
            )},
        {local_topic,
            sc(
                binary(),
                #{
                    desc => ?DESC("local_topic")
                }
            )},
        {pubsub_topic,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("pubsub_topic")
                }
            )}
    ];
fields(consumer) ->
    [
        %% Note: The minimum deadline pubsub does is 10 s.
        {ack_deadline,
            mk(
                emqx_schema:timeout_duration_s(),
                #{
                    default => <<"60s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {ack_retry_interval,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"5s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {pull_max_messages,
            mk(
                pos_integer(),
                #{default => 100, desc => ?DESC("consumer_pull_max_messages")}
            )},
        {consumer_workers_per_topic,
            mk(
                pos_integer(),
                #{
                    default => 1,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {topic_mapping,
            mk(
                hoconsc:array(ref(consumer_topic_mapping)),
                #{
                    required => true,
                    validator => fun consumer_topic_mapping_validator/1,
                    desc => ?DESC("consumer_topic_mapping")
                }
            )}
    ];
fields(consumer_topic_mapping) ->
    [
        {pubsub_topic, mk(binary(), #{required => true, desc => ?DESC(consumer_pubsub_topic)})},
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
fields("consumer_resource_opts") ->
    ResourceFields =
        emqx_resource_schema:create_opts(
            [{health_check_interval, #{default => <<"30s">>}}]
        ),
    SupportedFields = [
        health_check_interval,
        request_ttl
    ],
    lists:filter(
        fun({Field, _Sc}) -> lists:member(Field, SupportedFields) end,
        ResourceFields
    );
fields(key_value_pair) ->
    [
        {key,
            mk(emqx_schema:template(), #{
                required => true,
                validator => [
                    emqx_resource_validator:not_empty("Key templates must not be empty")
                ],
                desc => ?DESC(kv_pair_key)
            })},
        {value,
            mk(emqx_schema:template(), #{
                required => true,
                desc => ?DESC(kv_pair_value)
            })}
    ];
fields("get_producer") ->
    emqx_bridge_schema:status_fields() ++ fields("post_producer");
fields("post_producer") ->
    [type_field_producer(), name_field() | fields("config_producer")];
fields("put_producer") ->
    fields("config_producer");
fields("get_consumer") ->
    emqx_bridge_schema:status_fields() ++ fields("post_consumer");
fields("post_consumer") ->
    [type_field_consumer(), name_field() | fields("config_consumer")];
fields("put_consumer") ->
    fields("config_consumer").

desc("config_producer") ->
    ?DESC("desc_config");
desc(key_value_pair) ->
    ?DESC("kv_pair_desc");
desc("config_consumer") ->
    ?DESC("desc_config");
desc("consumer_resource_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(consumer_topic_mapping) ->
    ?DESC("consumer_topic_mapping");
desc(consumer) ->
    ?DESC("consumer");
desc(_) ->
    undefined.

conn_bridge_examples(Method) ->
    [
        #{
            <<"gcp_pubsub">> => #{
                summary => <<"GCP PubSub Producer Bridge">>,
                value => values(producer, Method)
            }
        },
        #{
            <<"gcp_pubsub_consumer">> => #{
                summary => <<"GCP PubSub Consumer Bridge">>,
                value => values(consumer, Method)
            }
        }
    ].

values(producer, _Method) ->
    #{
        pubsub_topic => <<"mytopic">>,
        service_account_json =>
            #{
                auth_provider_x509_cert_url =>
                    <<"https://www.googleapis.com/oauth2/v1/certs">>,
                auth_uri =>
                    <<"https://accounts.google.com/o/oauth2/auth">>,
                client_email =>
                    <<"test@myproject.iam.gserviceaccount.com">>,
                client_id => <<"123812831923812319190">>,
                client_x509_cert_url =>
                    <<
                        "https://www.googleapis.com/robot/v1/"
                        "metadata/x509/test%40myproject.iam.gserviceaccount.com"
                    >>,
                private_key =>
                    <<
                        "-----BEGIN PRIVATE KEY-----\n"
                        "MIIEvQI..."
                    >>,
                private_key_id => <<"kid">>,
                project_id => <<"myproject">>,
                token_uri =>
                    <<"https://oauth2.googleapis.com/token">>,
                type => <<"service_account">>
            }
    };
values(consumer, _Method) ->
    #{
        connect_timeout => <<"15s">>,
        consumer =>
            #{
                pull_max_messages => 100,
                topic_mapping => [
                    #{
                        pubsub_topic => <<"pubsub-topic-1">>,
                        mqtt_topic => <<"mqtt/topic/1">>,
                        qos => 1,
                        payload_template => <<"${.}">>
                    },
                    #{
                        pubsub_topic => <<"pubsub-topic-2">>,
                        mqtt_topic => <<"mqtt/topic/2">>,
                        qos => 2,
                        payload_template =>
                            <<"v = ${.value}, a = ${.attributes}, o = ${.ordering_key}">>
                    }
                ]
            },
        resource_opts => #{request_ttl => <<"20s">>},
        service_account_json =>
            #{
                auth_provider_x509_cert_url =>
                    <<"https://www.googleapis.com/oauth2/v1/certs">>,
                auth_uri =>
                    <<"https://accounts.google.com/o/oauth2/auth">>,
                client_email =>
                    <<"test@myproject.iam.gserviceaccount.com">>,
                client_id => <<"123812831923812319190">>,
                client_x509_cert_url =>
                    <<
                        "https://www.googleapis.com/robot/v1/"
                        "metadata/x509/test%40myproject.iam.gserviceaccount.com"
                    >>,
                private_key =>
                    <<
                        "-----BEGIN PRIVATE KEY-----\n"
                        "MIIEvQI..."
                    >>,
                private_key_id => <<"kid">>,
                project_id => <<"myproject">>,
                token_uri =>
                    <<"https://oauth2.googleapis.com/token">>,
                type => <<"service_account">>
            }
    }.

upgrade_raw_conf(RawConf0) ->
    lists:foldl(
        fun(Path, Acc) ->
            deep_update(
                Path,
                fun ensure_binary_service_account_json/1,
                Acc
            )
        end,
        RawConf0,
        [
            [<<"connectors">>, <<"gcp_pubsub_producer">>],
            [<<"connectors">>, <<"gcp_pubsub_consumer">>]
        ]
    ).

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

ref(Name) -> hoconsc:ref(?MODULE, Name).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

type_field_producer() ->
    {type, mk(enum([gcp_pubsub]), #{required => true, desc => ?DESC("desc_type")})}.

type_field_consumer() ->
    {type, mk(enum([gcp_pubsub_consumer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

-spec service_account_json_validator(binary()) ->
    ok
    | {error, {wrong_type, term()}}
    | {error, {missing_keys, [binary()]}}.
service_account_json_validator(Val) ->
    case emqx_utils_json:safe_decode(Val, [return_maps]) of
        {ok, Map} ->
            Map = emqx_utils_json:decode(Val, [return_maps]),
            ExpectedKeys = [
                <<"type">>,
                <<"project_id">>,
                <<"private_key_id">>,
                <<"private_key">>,
                <<"client_email">>
            ],
            MissingKeys = lists:sort([
                K
             || K <- ExpectedKeys,
                not maps:is_key(K, Map)
            ]),
            Type = maps:get(<<"type">>, Map, null),
            case {MissingKeys, Type} of
                {[], <<"service_account">>} ->
                    ok;
                {[], Type} ->
                    {error, #{wrong_type => Type}};
                {_, _} ->
                    {error, #{missing_keys => MissingKeys}}
            end;
        {error, _} ->
            {error, "not a json"}
    end.

service_account_json_converter(Val, #{make_serializable := true}) ->
    case is_map(Val) of
        true -> emqx_utils_json:encode(Val);
        false -> Val
    end;
service_account_json_converter(Map, _Opts) when is_map(Map) ->
    emqx_utils_json:encode(Map);
service_account_json_converter(Val, _Opts) ->
    case emqx_utils_json:safe_decode(Val, [return_maps]) of
        {ok, Str} when is_binary(Str) ->
            emqx_utils_json:decode(Str, [return_maps]);
        _ ->
            Val
    end.

consumer_topic_mapping_validator(_TopicMapping = []) ->
    {error, "There must be at least one GCP PubSub-MQTT topic mapping"};
consumer_topic_mapping_validator(TopicMapping0 = [_ | _]) ->
    TopicMapping = [emqx_utils_maps:binary_key_map(TM) || TM <- TopicMapping0],
    NumEntries = length(TopicMapping),
    PubSubTopics = [KT || #{<<"pubsub_topic">> := KT} <- TopicMapping],
    DistinctPubSubTopics = length(lists:usort(PubSubTopics)),
    case DistinctPubSubTopics =:= NumEntries of
        true ->
            ok;
        false ->
            {error, "GCP PubSub topics must not be repeated in a bridge"}
    end.

deep_update(Path, Fun, Map) ->
    case emqx_utils_maps:deep_get(Path, Map, #{}) of
        M when map_size(M) > 0 ->
            NewM = Fun(M),
            emqx_utils_maps:deep_put(Path, Map, NewM);
        _ ->
            Map
    end.

ensure_binary_service_account_json(Connectors) ->
    maps:map(
        fun(_Name, Conf) ->
            maps:update_with(
                <<"service_account_json">>,
                fun(JSON) ->
                    case is_map(JSON) of
                        true -> emqx_utils_json:encode(JSON);
                        false -> JSON
                    end
                end,
                Conf
            )
        end,
        Connectors
    ).
