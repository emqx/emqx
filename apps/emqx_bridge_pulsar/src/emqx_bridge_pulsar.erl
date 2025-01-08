%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).
%% emqx_bridge_enterprise "unofficial" API
-export([conn_bridge_examples/1]).

-export([producer_strategy_key_validator/1]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_pulsar".

roots() ->
    [].

fields(pulsar_producer) ->
    fields(config) ++
        emqx_bridge_pulsar_pubsub_schema:fields(action_parameters) ++
        [
            {local_topic,
                mk(binary(), #{required => false, desc => ?DESC("producer_local_topic")})},
            {resource_opts,
                mk(
                    ref(producer_resource_opts),
                    #{
                        required => false,
                        desc => ?DESC(emqx_resource_schema, "creation_opts")
                    }
                )}
        ];
fields(config) ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {servers,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("servers"),
                    validator => emqx_schema:servers_validator(
                        ?PULSAR_HOST_OPTIONS, _Required = true
                    )
                }
            )},
        {authentication,
            mk(
                hoconsc:union(fun auth_union_member_selector/1),
                #{
                    default => none,
                    %% must mark this whole union as sensitive because
                    %% hocon ignores the `sensitive' metadata in struct
                    %% fields...  Also, when trying to type check a struct
                    %% that doesn't match the intended type, it won't have
                    %% sensitivity information from sibling types.
                    sensitive => true,
                    desc => ?DESC("authentication")
                }
            )},
        {connect_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"5s">>,
                    desc => ?DESC("connect_timeout")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields(producer_opts) ->
    [
        {pulsar_topic, mk(string(), #{required => true, desc => ?DESC("producer_pulsar_topic")})},
        {batch_size,
            mk(
                pos_integer(),
                #{default => 100, desc => ?DESC("producer_batch_size")}
            )},
        {compression,
            mk(
                hoconsc:enum([no_compression, snappy, zlib]),
                #{default => no_compression, desc => ?DESC("producer_compression")}
            )},
        {send_buffer,
            mk(emqx_schema:bytesize(), #{
                default => <<"1MB">>, desc => ?DESC("producer_send_buffer")
            })},
        {retention_period,
            mk(
                %% not used in a `receive ... after' block, just timestamp comparison
                hoconsc:union([infinity, emqx_schema:duration_ms()]),
                #{default => infinity, desc => ?DESC("producer_retention_period")}
            )},
        {max_batch_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"900KB">>, desc => ?DESC("producer_max_batch_bytes")}
            )},
        {strategy,
            mk(
                hoconsc:enum([random, roundrobin, key_dispatch]),
                #{default => random, desc => ?DESC("producer_strategy")}
            )},
        {buffer, mk(ref(producer_buffer), #{required => false, desc => ?DESC("producer_buffer")})}
    ];
fields(producer_buffer) ->
    [
        {mode,
            mk(
                hoconsc:enum([memory, disk, hybrid]),
                #{default => memory, desc => ?DESC("buffer_mode")}
            )},
        {per_partition_limit,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"2GB">>, desc => ?DESC("buffer_per_partition_limit")}
            )},
        {segment_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"100MB">>, desc => ?DESC("buffer_segment_bytes")}
            )},
        {memory_overload_protection,
            mk(boolean(), #{
                default => false,
                desc => ?DESC("buffer_memory_overload_protection")
            })}
    ];
fields(producer_resource_opts) ->
    SupportedOpts = [
        health_check_interval,
        resume_interval,
        start_after_created,
        start_timeout
    ],
    lists:filtermap(
        fun
            ({health_check_interval = Field, MetaFn}) ->
                {true, {Field, override_default(MetaFn, <<"1s">>)}};
            ({Field, _Meta}) ->
                lists:member(Field, SupportedOpts)
        end,
        emqx_resource_schema:fields("creation_opts")
    );
fields(auth_basic) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("auth_basic_username")})},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("auth_basic_password")
            })}
    ];
fields(auth_token) ->
    [
        {jwt,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("auth_token_jwt")
            })}
    ];
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("post_" ++ Type) ->
    [type_field(), name_field() | fields("config_" ++ Type)];
fields("config_producer") ->
    fields(pulsar_producer).

desc(pulsar_producer) ->
    ?DESC(pulsar_producer_struct);
desc(producer_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("get_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `GET` method."];
desc("put_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `PUT` method."];
desc("post_" ++ Type) when Type =:= "producer" ->
    ["Configuration for Pulsar using `POST` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

conn_bridge_examples(_Method) ->
    [
        #{
            <<"pulsar_producer">> => #{
                summary => <<"Pulsar Producer Bridge">>,
                value => #{
                    <<"authentication">> => <<"none">>,
                    <<"batch_size">> => 1,
                    <<"buffer">> =>
                        #{
                            <<"memory_overload_protection">> => true,
                            <<"mode">> => <<"memory">>,
                            <<"per_partition_limit">> => <<"10MB">>,
                            <<"segment_bytes">> => <<"5MB">>
                        },
                    <<"compression">> => <<"no_compression">>,
                    <<"enable">> => true,
                    <<"local_topic">> => <<"mqtt/topic/-576460752303423482">>,
                    <<"max_batch_bytes">> => <<"900KB">>,
                    <<"message">> =>
                        #{<<"key">> => <<"${.clientid}">>, <<"value">> => <<"${.}">>},
                    <<"name">> => <<"pulsar_example_name">>,
                    <<"pulsar_topic">> => <<"pulsar_example_topic">>,
                    <<"retention_period">> => <<"infinity">>,
                    <<"send_buffer">> => <<"1MB">>,
                    <<"servers">> => <<"pulsar://127.0.0.1:6650">>,
                    <<"ssl">> =>
                        #{
                            <<"enable">> => false,
                            <<"server_name_indication">> => <<"auto">>,
                            <<"verify">> => <<"verify_none">>
                        },
                    <<"strategy">> => <<"key_dispatch">>,
                    <<"sync_timeout">> => <<"5s">>,
                    <<"type">> => <<"pulsar_producer">>
                }
            }
        }
    ].

producer_strategy_key_validator(
    #{
        strategy := _,
        message := #{key := _}
    } = Conf
) ->
    producer_strategy_key_validator(emqx_utils_maps:binary_key_map(Conf));
producer_strategy_key_validator(#{
    <<"strategy">> := key_dispatch,
    <<"message">> := #{<<"key">> := Key}
}) when Key =:= "" orelse Key =:= <<>> ->
    {error, "Message key cannot be empty when `key_dispatch` strategy is used"};
producer_strategy_key_validator(_) ->
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

type_field() ->
    {type, mk(hoconsc:enum([pulsar_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

struct_names() ->
    [
        auth_basic,
        auth_token,
        producer_buffer
    ].

override_default(OriginalFn, NewDefault) ->
    fun
        (default) -> NewDefault;
        (Field) -> OriginalFn(Field)
    end.

auth_union_member_selector(all_union_members) ->
    [none, ref(auth_basic), ref(auth_token)];
auth_union_member_selector({value, V0}) ->
    V =
        case is_map(V0) of
            true -> emqx_utils_maps:binary_key_map(V0);
            false -> V0
        end,
    case V of
        #{<<"password">> := _} ->
            [ref(auth_basic)];
        #{<<"jwt">> := _} ->
            [ref(auth_token)];
        <<"none">> ->
            [none];
        none ->
            [none];
        _ ->
            Expected = "none | basic | token",
            throw(#{
                field_name => authentication,
                expected => Expected
            })
    end.
