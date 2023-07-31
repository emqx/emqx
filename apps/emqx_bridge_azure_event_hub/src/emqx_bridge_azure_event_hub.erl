%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_hub).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_bridge_resource).

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% emqx_bridge_enterprise "unofficial" API
-export([conn_bridge_examples/1]).

-export([connector_config/2]).

-export([producer_converter/2, host_opts/0]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "bridge_azure_event_hub".

roots() -> ["config_producer"].

fields("post_producer") ->
    Fields = override(
        emqx_bridge_kafka:fields("post_producer"),
        producer_overrides()
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
fields("ssl_client_opts") ->
    Fields = override(
        emqx_schema:fields("ssl_client_opts"),
        ssl_overrides()
    ),
    override_documentations(Fields);
fields(producer_kafka_opts) ->
    Fields = override(
        emqx_bridge_kafka:fields(producer_kafka_opts),
        kafka_producer_overrides()
    ),
    override_documentations(Fields);
fields(Method) ->
    Fields = emqx_bridge_kafka:fields(Method),
    override_documentations(Fields).

desc("config_producer") ->
    ?DESC("desc_config");
desc("ssl_client_opts") ->
    emqx_schema:desc("ssl_client_opts");
desc("get_producer") ->
    ["Configuration for Azure Event Hub using `GET` method."];
desc("put_producer") ->
    ["Configuration for Azure Event Hub using `PUT` method."];
desc("post_producer") ->
    ["Configuration for Azure Event Hub using `POST` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

struct_names() ->
    [
        auth_username_password,
        producer_kafka_opts
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

values({get, AEHType}) ->
    values({post, AEHType});
values({post, AEHType}) ->
    maps:merge(values(common_config), values(AEHType));
values({put, AEHType}) ->
    values({post, AEHType});
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
        kafka => #{
            topic => <<"topic">>,
            message => #{
                key => <<"${.clientid}">>,
                value => <<"${.}">>,
                timestamp => <<"${.timestamp}">>
            },
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
%% `emqx_bridge_resource' API
%%-------------------------------------------------------------------------------------------------

connector_config(Config, BridgeName) ->
    %% Default port for AEH is 9093
    BootstrapHosts0 = maps:get(bootstrap_hosts, Config),
    BootstrapHosts = emqx_schema:parse_servers(
        BootstrapHosts0,
        emqx_bridge_azure_event_hub:host_opts()
    ),
    Config#{bridge_name => BridgeName, bootstrap_hosts := BootstrapHosts}.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

ref(Name) ->
    hoconsc:ref(?MODULE, Name).

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
        kafka =>
            mk(ref(producer_kafka_opts), #{
                required => true,
                validator => fun emqx_bridge_kafka:producer_strategy_key_validator/1
            }),
        ssl => mk(ref("ssl_client_opts"), #{default => #{<<"enable">> => true}}),
        type => mk(azure_event_hub_producer, #{required => true})
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

ssl_overrides() ->
    #{
        "cacerts" => mk(boolean(), #{default => true}),
        "enable" => mk(true, #{default => true}),
        "server_name_indication" =>
            mk(
                hoconsc:union([disable, auto, string()]),
                #{
                    example => auto,
                    default => <<"auto">>
                }
            )
    }.

kafka_producer_overrides() ->
    #{
        compression =>
            mk(no_compression, #{
                default => no_compression,
                importance => ?IMPORTANCE_HIDDEN
            }),
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
