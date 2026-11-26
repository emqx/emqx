%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    connector_examples/1
]).

-export([validate_credentials/1]).

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
    fields(producer);
fields(kinesis_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                validator => fun validate_stream/1,
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
                default => 100,
                validator => emqx_resource_validator:max(int, 500)
            }},
            {batch_time, #{default => <<"100ms">>}},
            {health_check_interval_jitter, #{
                default => <<"15s">>
            }}
        ]
    );
fields(connector_config) ->
    [
        {aws_access_key_id,
            mk(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("aws_access_key_id")
                }
            )},
        {aws_secret_access_key,
            emqx_schema_secret:mk(
                #{
                    required => false,
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
                    required => false,
                    desc => ?DESC("stream_name")
                }
            )},
        {stream_arn,
            sc(
                binary(),
                #{
                    required => false,
                    example => <<"arn:aws:kinesis:us-east-1:123456789012:stream/my_stream">>,
                    validator => fun validate_stream_arn/1,
                    desc => ?DESC("stream_arn")
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
    ];
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

%%-------------------------------------------------------------------------------------------------
%% Validators
%%-------------------------------------------------------------------------------------------------

%% Without static credentials, the connector obtains them from the ECS task role or EC2 instance
%% metadata, so the access key ID and the secret access key must be either both set or both unset.
%%
%% Hocon calls the validator of the `connectors.kinesis' map both with the whole map and with each
%% connector config in it, whose keys may be either atoms or binaries.
validate_credentials(Config) ->
    case get_field(endpoint, Config) of
        Endpoint when is_binary(Endpoint) ->
            HasAccessKeyId = is_set(get_field(aws_access_key_id, Config)),
            HasSecretAccessKey = is_set(
                emqx_schema_secret:source(get_field(aws_secret_access_key, Config))
            ),
            case HasAccessKeyId =:= HasSecretAccessKey of
                true ->
                    ok;
                false ->
                    {error,
                        <<"aws_access_key_id and aws_secret_access_key must be provided together">>}
            end;
        _ ->
            %% Map level validation
            ok
    end.

%% The action identifies the stream either by its name, in the account of the connector
%% credentials, or by its ARN.
validate_stream(Parameters) ->
    case {is_set(get_field(stream_name, Parameters)), is_set(get_field(stream_arn, Parameters))} of
        {true, false} -> ok;
        {false, true} -> ok;
        {true, true} -> {error, <<"stream_name and stream_arn cannot be set together">>};
        {false, false} -> {error, <<"one of stream_name and stream_arn must be set">>}
    end.

%% <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html>
validate_stream_arn(undefined) ->
    ok;
validate_stream_arn(<<>>) ->
    ok;
validate_stream_arn(StreamARN) ->
    Pattern = "^arn:aws[a-z-]*:kinesis:[a-z0-9-]+:[0-9]{12}:stream/[a-zA-Z0-9_.-]{1,128}$",
    case re:run(StreamARN, Pattern, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error,
                <<"must be a Kinesis stream ARN, such as ",
                    "arn:aws:kinesis:us-east-1:123456789012:stream/my_stream">>}
    end.

get_field(Key, Config) ->
    maps:get(Key, Config, maps:get(atom_to_binary(Key), Config, undefined)).

is_set(undefined) -> false;
is_set(<<>>) -> false;
is_set(_) -> true.

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
