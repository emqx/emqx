%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_gcp_pubsub).

-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).
-export([
    service_account_json_validator/1,
    service_account_json_converter/1
]).

%% emqx_ee_bridge "unofficial" API
-export([conn_bridge_examples/1]).

-type service_account_json() :: map().
-reflect_type([service_account_json/0]).

-define(DEFAULT_PIPELINE_SIZE, 100).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_gcp_pubsub".

roots() ->
    [].

fields("config") ->
    emqx_bridge_schema:common_bridge_fields() ++
        emqx_resource_schema:fields("resource_opts") ++
        fields(bridge_config);
fields(bridge_config) ->
    [
        {connect_timeout,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => "15s",
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
                emqx_schema:duration_ms(),
                #{
                    required => false,
                    default => "15s",
                    desc => ?DESC("request_timeout")
                }
            )},
        {payload_template,
            sc(
                binary(),
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
            )},
        {service_account_json,
            sc(
                service_account_json(),
                #{
                    required => true,
                    validator => fun ?MODULE:service_account_json_validator/1,
                    converter => fun ?MODULE:service_account_json_converter/1,
                    sensitive => true,
                    desc => ?DESC("service_account_json")
                }
            )}
    ];
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config").

desc("config") ->
    ?DESC("desc_config");
desc(_) ->
    undefined.

conn_bridge_examples(Method) ->
    [
        #{
            <<"gcp_pubsub">> => #{
                summary => <<"GCP PubSub Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(values(post), ?METRICS_EXAMPLE);
values(post) ->
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
values(put) ->
    values(post).

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

type_field() ->
    {type, mk(enum([gcp_pubsub]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

-spec service_account_json_validator(map()) ->
    ok
    | {error, {wrong_type, term()}}
    | {error, {missing_keys, [binary()]}}.
service_account_json_validator(Map) ->
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
            {error, {wrong_type, Type}};
        {_, _} ->
            {error, {missing_keys, MissingKeys}}
    end.

service_account_json_converter(Map) when is_map(Map) ->
    ExpectedKeys = [
        <<"type">>,
        <<"project_id">>,
        <<"private_key_id">>,
        <<"private_key">>,
        <<"client_email">>
    ],
    maps:with(ExpectedKeys, Map);
service_account_json_converter(Val) ->
    Val.
