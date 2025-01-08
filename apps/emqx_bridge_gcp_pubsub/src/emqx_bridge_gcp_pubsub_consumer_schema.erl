%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_schema).

-behaviour(emqx_connector_examples).

-import(hoconsc, [mk/2, ref/2]).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    source_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, gcp_pubsub_consumer).
-define(SOURCE_TYPE, gcp_pubsub_consumer).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "gcp_pubsub_consumer".

roots() ->
    [].

%%=========================================
%% Action fields
%%=========================================
fields(source) ->
    {gcp_pubsub_consumer,
        mk(
            hoconsc:map(name, ref(?MODULE, consumer_source)),
            #{
                desc => <<"GCP PubSub Consumer Source Config">>,
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
        ),
        #{resource_opts_ref => ref(?MODULE, source_resource_opts)}
    );
fields(source_parameters) ->
    Fields0 = emqx_bridge_gcp_pubsub:fields(consumer),
    Fields = lists:map(
        fun
            ({topic_mapping = Name, Sc}) ->
                %% to please dialyzer...
                Override = #{
                    type => hocon_schema:field_schema(Sc, type),
                    required => false,
                    default => [],
                    validator => fun(_) -> ok end,
                    importance => ?IMPORTANCE_HIDDEN
                },
                {Name, hocon_schema:override(Sc, Override)};
            (FieldSchema) ->
                FieldSchema
        end,
        Fields0
    ),
    [
        {topic,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "pubsub_topic")
                }
            )}
        | Fields
    ];
fields(source_resource_opts) ->
    Fields = [
        health_check_interval,
        %% the workers pull the messages
        request_ttl,
        resume_interval
    ],
    lists:filter(
        fun({Key, _Sc}) -> lists:member(Key, Fields) end,
        emqx_resource_schema:create_opts(
            _Overrides = [
                {health_check_interval, #{default => <<"30s">>}}
            ]
        )
    );
%%=========================================
%% Connector fields
%%=========================================
fields("config_connector") ->
    %% FIXME
    emqx_connector_schema:common_fields() ++
        connector_config_fields();
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
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
%% HTTP API fields: connector
%%=========================================
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, connector_config_fields()).

connector_config_fields() ->
    emqx_bridge_gcp_pubsub:fields(connector_config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts).

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
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

source_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_consumer">> => #{
                summary => <<"GCP PubSub Consumer Source">>,
                value => source_example(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_consumer">> => #{
                summary => <<"GCP PubSub Consumer Connector">>,
                value => connector_example(Method)
            }
        }
    ].

conn_bridge_examples(Method) ->
    emqx_bridge_gcp_pubsub:conn_bridge_examples(Method).

source_example(post) ->
    maps:merge(
        source_example(put),
        #{
            type => <<"gcp_pubsub_consumer">>,
            name => <<"my_action">>
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
        enable => true,
        description => <<"my source">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                topic => <<"my-topic">>,
                pull_max_messages => 100
            },
        resource_opts =>
            #{
                request_ttl => <<"45s">>,
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
            ],
            actions => [<<"my_action">>]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => <<"gcp_pubsub_producer">>,
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"my connector">>,
        connect_timeout => <<"15s">>,
        pool_size => 8,
        resource_opts =>
            #{
                start_after_created => true,
                health_check_interval => <<"30s">>,
                start_timeout => <<"5s">>
            },
        max_retries => 2,
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
            },
        pipelining => 100
    }.
