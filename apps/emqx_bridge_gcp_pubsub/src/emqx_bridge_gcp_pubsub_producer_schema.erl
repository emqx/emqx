%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_producer_schema).

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
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, gcp_pubsub_producer).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "gcp_pubsub_producer".

roots() ->
    [].

%%=========================================
%% Action fields
%%=========================================
fields(action) ->
    {gcp_pubsub_producer,
        mk(
            hoconsc:map(name, ref(?MODULE, producer_action)),
            #{
                desc => <<"GCP PubSub Producer Action Config">>,
                required => false
            }
        )};
fields(producer_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC(producer_action)
            }
        )
    );
fields(action_parameters) ->
    lists:map(
        fun
            ({local_topic, Sc}) ->
                Override = #{
                    %% to please dialyzer...
                    type => hocon_schema:field_schema(Sc, type),
                    importance => ?IMPORTANCE_HIDDEN
                },
                {local_topic, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        emqx_bridge_gcp_pubsub:fields(producer)
    );
%%=========================================
%% Connector fields
%%=========================================
fields("config_connector") ->
    %% FIXME
    emqx_connector_schema:common_fields() ++
        connector_config_fields();
fields(connector_resource_opts) ->
    %% for backwards compatibility...
    Fields = proplists:get_keys(emqx_connector_schema:resource_opts_fields()),
    AllFields = proplists:get_keys(emqx_resource_schema:create_opts([])),
    DeprecatedFields = AllFields -- Fields,
    Overrides = lists:map(
        fun(Field) ->
            {Field, #{
                importance => ?IMPORTANCE_HIDDEN,
                deprecated => {since, "5.5.0"}
            }}
        end,
        DeprecatedFields
    ),
    emqx_resource_schema:create_opts(Overrides);
%%=========================================
%% HTTP API fields: action
%%=========================================
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2");
fields("post_bridge_v2") ->
    [type_field(), name_field() | fields("put_bridge_v2")];
fields("put_bridge_v2") ->
    fields(producer_action);
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
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(producer_action) ->
    ?DESC(producer_action);
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_Name) ->
    undefined.

type_field() ->
    {type, mk(gcp_pubsub_producer, #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_producer">> => #{
                summary => <<"GCP PubSub Producer Action">>,
                value => action_example(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"gcp_pubsub_producer">> => #{
                summary => <<"GCP PubSub Producer Connector">>,
                value => connector_example(Method)
            }
        }
    ].

conn_bridge_examples(Method) ->
    emqx_bridge_gcp_pubsub:conn_bridge_examples(Method).

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => <<"gcp_pubsub_producer">>,
            name => <<"my_action">>
        }
    );
action_example(get) ->
    maps:merge(
        action_example(put),
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
action_example(put) ->
    #{
        enable => true,
        connector => <<"my_connector_name">>,
        description => <<"My action">>,
        local_topic => <<"local/topic">>,
        resource_opts =>
            #{batch_size => 5},
        parameters =>
            #{
                pubsub_topic => <<"mytopic">>,
                ordering_key_template => <<"${payload.ok}">>,
                payload_template => <<"${payload}">>,
                attributes_template =>
                    [
                        #{
                            key => <<"${payload.attrs.k}">>,
                            value => <<"${payload.attrs.v}">>
                        }
                    ]
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
        connect_timeout => <<"10s">>,
        pool_size => 8,
        pipelining => 100,
        max_retries => 2,
        resource_opts => #{request_ttl => <<"60s">>},
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
