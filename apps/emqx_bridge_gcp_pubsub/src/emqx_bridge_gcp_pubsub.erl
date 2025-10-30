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

-define(DEFAULT_PIPELINE_SIZE, 100).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_gcp_pubsub".

roots() ->
    [].

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
        emqx_connector_schema:ehttpc_max_inactive_sc(),
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
            emqx_schema_secret:mk(
                #{
                    required => true,
                    validator => fun ?MODULE:service_account_json_validator/1,
                    converter => fun ?MODULE:service_account_json_converter/2,
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
    emqx_bridge_v2_api:status_fields() ++ fields("post_producer");
fields("post_producer") ->
    [type_field_producer(), name_field() | fields("config_producer")];
fields("put_producer") ->
    fields("config_producer");
fields("get_consumer") ->
    emqx_bridge_v2_api:status_fields() ++ fields("post_consumer");
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
desc(consumer) ->
    ?DESC("consumer");
desc(_) ->
    undefined.

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

-spec service_account_json_validator(term()) ->
    ok
    | {error, {wrong_type, term()}}
    | {error, {missing_keys, [binary()]}}
    | {error, term()}.
service_account_json_validator(Val) when is_function(Val, 0) ->
    %% This is a wrapped secret (e.g., file reference)
    %% We can't validate the content at schema validation time
    %% The actual validation will happen when the secret is unwrapped
    ok;
service_account_json_validator(Val) when is_binary(Val) ->
    %% Check if it's a file reference
    case Val of
        <<"file://", _/binary>> ->
            %% File reference - validation will happen at runtime
            ok;
        _ ->
            %% Inline JSON - validate now
            validate_service_account_json_content(Val)
    end;
service_account_json_validator(Val) when is_map(Val) ->
    %% Already decoded JSON map
    validate_service_account_json_map(Val);
service_account_json_validator(_Val) ->
    {error, "invalid type for service_account_json"}.

validate_service_account_json_content(Val) ->
    case emqx_utils_json:safe_decode(Val) of
        {ok, Map} ->
            validate_service_account_json_map(Map);
        {error, _} ->
            {error, "not a json"}
    end.

validate_service_account_json_map(Map) ->
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
    end.

service_account_json_converter(Val, #{make_serializable := true}) ->
    %% When serializing, we need to get the source representation
    case Val of
        Fun when is_function(Fun, 0) ->
            %% This is a wrapped secret - get its source representation
            emqx_schema_secret:source(Fun);
        Map when is_map(Map) ->
            emqx_utils_json:encode(Map);
        Bin when is_binary(Bin) ->
            Bin
    end;
service_account_json_converter(Val, _Opts) when is_function(Val, 0) ->
    %% This is already a wrapped secret from emqx_schema_secret
    Val;
service_account_json_converter(Map, Opts) when is_map(Map) ->
    %% Convert map to JSON string then wrap it
    JsonBin = emqx_utils_json:encode(Map),
    emqx_schema_secret:convert_secret(JsonBin, Opts);
service_account_json_converter(Val, Opts) when is_binary(Val) ->
    %% This could be either a file reference or inline JSON
    %% Let emqx_schema_secret handle it
    case Val of
        <<"file://", _/binary>> ->
            %% File reference - let emqx_schema_secret wrap it
            emqx_schema_secret:convert_secret(Val, Opts);
        _ ->
            %% Inline JSON - validate it's proper JSON first
            case emqx_utils_json:safe_decode(Val) of
                {ok, Decoded} when is_map(Decoded) ->
                    %% Valid JSON - wrap it
                    emqx_schema_secret:convert_secret(Val, Opts);
                {ok, Str} when is_binary(Str) ->
                    %% Double-encoded JSON string
                    emqx_schema_secret:convert_secret(emqx_utils_json:decode(Str), Opts);
                {error, _} ->
                    %% Not valid JSON - return as is for error handling
                    Val
            end
    end;
service_account_json_converter(Val, _Opts) ->
    Val.

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
