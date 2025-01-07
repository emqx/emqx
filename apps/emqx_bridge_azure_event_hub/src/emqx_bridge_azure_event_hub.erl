%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_hub).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_connector_resource).

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

%% emqx_connector_resource behaviour callbacks
-export([connector_config/2]).

-export([producer_converter/2, host_opts/0]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(CONNECTOR_TYPE, azure_event_hub_producer).
-define(CONNECTOR_TYPE_BIN, <<"azure_event_hub_producer">>).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "bridge_azure_event_hub".

roots() -> ["config_producer"].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields = override(
        emqx_connector_schema:api_fields(
            Field,
            ?CONNECTOR_TYPE,
            emqx_bridge_kafka:kafka_connector_config_fields()
        ),
        connector_overrides()
    ),
    override_documentations(Fields);
fields("put_bridge_v2") ->
    Fields = override(
        emqx_bridge_kafka:fields("put_bridge_v2"),
        bridge_v2_overrides()
    ),
    override_documentations(Fields);
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++
        fields("post_bridge_v2");
fields("post_bridge_v2") ->
    Fields = override(
        emqx_bridge_kafka:fields("post_bridge_v2"),
        bridge_v2_overrides()
    ),
    override_documentations(Fields);
fields("post_producer") ->
    Fields = override(
        emqx_bridge_kafka:fields("post_producer"),
        producer_overrides()
    ),
    override_documentations(Fields);
fields("config_bridge_v2") ->
    fields(actions);
fields("config_connector") ->
    Fields = override(
        emqx_bridge_kafka:fields("config_connector"),
        connector_overrides()
    ),
    override_documentations(Fields);
fields("config_producer") ->
    Fields = override(
        emqx_bridge_kafka:fields(kafka_producer),
        producer_overrides()
    ),
    override_documentations(Fields);
fields(auth_username_password) ->
    Fields = override(
        emqx_bridge_kafka:fields(auth_username_password),
        auth_overrides()
    ),
    override_documentations(Fields);
fields(ssl_client_opts) ->
    Fields = override(
        emqx_bridge_kafka:ssl_client_opts_fields(),
        ssl_overrides()
    ),
    override_documentations(Fields);
fields(producer_kafka_opts) ->
    Fields = override(
        emqx_bridge_kafka:fields(producer_kafka_opts),
        kafka_producer_overrides()
    ),
    override_documentations(Fields);
fields(kafka_message) ->
    Fields0 = emqx_bridge_kafka:fields(kafka_message),
    Fields = proplists:delete(timestamp, Fields0),
    override_documentations(Fields);
fields(action) ->
    {azure_event_hub_producer,
        mk(
            hoconsc:map(name, ref(emqx_bridge_azure_event_hub, actions)),
            #{
                desc => <<"Azure Event Hub Actions Config">>,
                required => false
            }
        )};
fields(actions) ->
    Fields =
        override(
            emqx_bridge_kafka:producer_opts(action),
            bridge_v2_overrides()
        ) ++ emqx_bridge_v2_schema:common_fields(),
    override_documentations(Fields);
fields(Method) ->
    Fields = emqx_bridge_kafka:fields(Method),
    override_documentations(Fields).

desc("config") ->
    ?DESC("desc_config");
desc("config_connector") ->
    ?DESC("desc_config");
desc("config_producer") ->
    ?DESC("desc_config");
desc("get_" ++ Type) when Type == "producer"; Type == "connector"; Type == "bridge_v2" ->
    ["Configuration for Azure Event Hub using `GET` method."];
desc("put_" ++ Type) when Type == "producer"; Type == "connector"; Type == "bridge_v2" ->
    ["Configuration for Azure Event Hub using `PUT` method."];
desc("post_" ++ Type) when Type == "producer"; Type == "connector"; Type == "bridge_v2" ->
    ["Configuration for Azure Event Hub using `POST` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

struct_names() ->
    [
        auth_username_password,
        kafka_message,
        producer_kafka_opts,
        actions,
        ssl_client_opts
    ].

bridge_v2_examples(Method) ->
    [
        #{
            ?CONNECTOR_TYPE_BIN => #{
                summary => <<"Azure Event Hub Action">>,
                value => values({Method, bridge_v2})
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            ?CONNECTOR_TYPE_BIN => #{
                summary => <<"Azure Event Hub Connector">>,
                value => values({Method, connector})
            }
        }
    ].

conn_bridge_examples(Method) ->
    [
        #{
            <<"azure_event_hub_producer">> => #{
                summary => <<"Azure Event Hub Producer Bridge">>,
                value => values({Method, producer})
            }
        }
    ].

values({get, connector}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values({post, connector})
    );
values({get, AEHType}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values({post, AEHType})
    );
values({post, bridge_v2}) ->
    maps:merge(
        values(producer),
        #{
            enable => true,
            connector => <<"my_azure_event_hub_producer_connector">>,
            name => <<"my_azure_event_hub_producer_action">>,
            type => ?CONNECTOR_TYPE_BIN
        }
    );
values({post, connector}) ->
    maps:merge(
        values(common_config),
        #{
            name => <<"my_azure_event_hub_producer_connector">>,
            type => ?CONNECTOR_TYPE_BIN,
            ssl => #{
                enable => true,
                server_name_indication => <<"auto">>,
                verify => <<"verify_none">>,
                versions => [<<"tlsv1.3">>, <<"tlsv1.2">>]
            }
        }
    );
values({post, producer}) ->
    maps:merge(
        #{
            name => <<"my_azure_event_hub_producer">>,
            type => <<"azure_event_hub_producer">>
        },
        maps:merge(
            values(common_config),
            values(producer)
        )
    );
values({put, connector}) ->
    values(common_config);
values({put, bridge_v2}) ->
    maps:merge(
        values(producer),
        #{
            enable => true,
            connector => <<"my_azure_event_hub_producer_connector">>
        }
    );
values({put, producer}) ->
    values({post, producer});
values(common_config) ->
    #{
        authentication => #{
            password => <<"******">>
        },
        bootstrap_hosts => <<"namespace.servicebus.windows.net:9093">>,
        connect_timeout => <<"5s">>,
        enable => true,
        metadata_request_timeout => <<"4s">>,
        min_metadata_refresh_interval => <<"3s">>,
        socket_opts => #{
            sndbuf => <<"1024KB">>,
            recbuf => <<"1024KB">>,
            nodelay => true,
            tcp_keepalive => <<"none">>
        }
    };
values(producer) ->
    #{
        parameters => #{
            topic => <<"topic">>,
            message => #{
                key => <<"${.clientid}">>,
                value => <<"${.}">>
            },
            max_linger_time => <<"5ms">>,
            max_linger_bytes => <<"10MB">>,
            max_batch_bytes => <<"896KB">>,
            partition_strategy => <<"random">>,
            required_acks => <<"all_isr">>,
            partition_count_refresh_interval => <<"60s">>,
            kafka_headers => <<"${.pub_props}">>,
            kafka_ext_headers => [
                #{
                    kafka_ext_header_key => <<"clientid">>,
                    kafka_ext_header_value => <<"${clientid}">>
                },
                #{
                    kafka_ext_header_key => <<"topic">>,
                    kafka_ext_header_value => <<"${topic}">>
                }
            ],
            kafka_header_value_encode_mode => none,
            max_inflight => 10,
            buffer => #{
                mode => <<"hybrid">>,
                per_partition_limit => <<"2GB">>,
                segment_bytes => <<"100MB">>,
                memory_overload_protection => true
            }
        },
        local_topic => <<"mqtt/local/topic">>
    }.

%%-------------------------------------------------------------------------------------------------
%% `emqx_connector_resource' API
%%-------------------------------------------------------------------------------------------------

connector_config(Config, _) ->
    %% Default port for AEH is 9093
    BootstrapHosts0 = maps:get(bootstrap_hosts, Config),
    BootstrapHosts = emqx_schema:parse_servers(
        BootstrapHosts0,
        emqx_bridge_azure_event_hub:host_opts()
    ),
    Config#{bootstrap_hosts := BootstrapHosts}.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

ref(Name) ->
    hoconsc:ref(?MODULE, Name).

connector_overrides() ->
    #{
        authentication =>
            mk(
                ref(auth_username_password),
                #{
                    default => #{},
                    required => true,
                    desc => ?DESC("authentication")
                }
            ),
        bootstrap_hosts =>
            mk(
                binary(),
                #{
                    required => true,
                    validator => emqx_schema:servers_validator(
                        host_opts(), _Required = true
                    )
                }
            ),
        ssl => mk(
            ref(ssl_client_opts),
            #{
                required => true,
                default => #{<<"enable">> => true}
            }
        ),
        type => mk(
            ?CONNECTOR_TYPE,
            #{
                required => true,
                desc => ?DESC("connector_type")
            }
        )
    }.

producer_overrides() ->
    #{
        authentication =>
            mk(
                ref(auth_username_password),
                #{
                    default => #{},
                    required => true,
                    desc => ?DESC("authentication")
                }
            ),
        bootstrap_hosts =>
            mk(
                binary(),
                #{
                    required => true,
                    validator => emqx_schema:servers_validator(
                        host_opts(), _Required = true
                    )
                }
            ),
        %% NOTE: field 'kafka' is renamed to 'parameters' since e5.3.1
        %% We will keep 'kafka' for backward compatibility.
        %% TODO: delete this override when we upgrade bridge schema json to 0.2.0
        %% See emqx_conf:bridge_schema_json/0
        kafka =>
            mk(ref(producer_kafka_opts), #{
                required => true,
                validator => fun emqx_bridge_kafka:producer_strategy_key_validator/1
            }),
        parameters =>
            mk(ref(producer_kafka_opts), #{
                required => true,
                validator => fun emqx_bridge_kafka:producer_strategy_key_validator/1
            }),
        ssl => mk(ref(ssl_client_opts), #{default => #{<<"enable">> => true}}),
        type => mk(azure_event_hub_producer, #{required => true})
    }.

bridge_v2_overrides() ->
    #{
        parameters =>
            mk(ref(producer_kafka_opts), #{
                required => true,
                validator => fun emqx_bridge_kafka:producer_parameters_validator/1
            }),
        ssl => mk(ref(ssl_client_opts), #{default => #{<<"enable">> => true}}),
        type => mk(
            ?CONNECTOR_TYPE,
            #{
                required => true,
                desc => ?DESC("bridge_v2_type")
            }
        )
    }.
auth_overrides() ->
    #{
        mechanism =>
            mk(plain, #{
                required => true,
                default => plain,
                importance => ?IMPORTANCE_HIDDEN
            }),
        username =>
            mk(binary(), #{
                required => true,
                default => <<"$ConnectionString">>,
                importance => ?IMPORTANCE_HIDDEN
            })
    }.

%% Kafka has SSL disabled by default
%% Azure must use SSL
ssl_overrides() ->
    #{
        "enable" => mk(true, #{default => true})
    }.

kafka_producer_overrides() ->
    #{
        compression =>
            mk(no_compression, #{
                default => no_compression,
                importance => ?IMPORTANCE_HIDDEN
            }),
        message => mk(ref(kafka_message), #{}),
        required_acks => mk(enum([all_isr, leader_only]), #{default => all_isr})
    }.

override_documentations(Fields) ->
    lists:map(
        fun({Name, Sc}) ->
            case hocon_schema:field_schema(Sc, desc) of
                ?DESC(emqx_bridge_kafka, Key) ->
                    %% to please dialyzer...
                    Override = #{type => hocon_schema:field_schema(Sc, type), desc => ?DESC(Key)},
                    {Name, hocon_schema:override(Sc, Override)};
                _ ->
                    {Name, Sc}
            end
        end,
        Fields
    ).

override(Fields, Overrides) ->
    lists:map(
        fun({Name, Sc}) ->
            case maps:find(Name, Overrides) of
                {ok, Override} ->
                    {Name, hocon_schema:override(Sc, Override)};
                error ->
                    {Name, Sc}
            end
        end,
        Fields
    ).

producer_converter(undefined, _HoconOpts) ->
    undefined;
producer_converter(
    Opts = #{<<"ssl">> := #{<<"server_name_indication">> := <<"auto">>}}, _HoconOpts
) ->
    %% Azure Event Hub's SNI is just the hostname without the Event Hub Namespace...
    emqx_utils_maps:deep_merge(
        Opts,
        #{<<"ssl">> => #{<<"server_name_indication">> => <<"servicebus.windows.net">>}}
    );
producer_converter(Opts, _HoconOpts) ->
    Opts.

host_opts() ->
    #{default_port => 9093}.
