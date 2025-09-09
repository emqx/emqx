%%-------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_connector_schema).

-feature(maybe_expr, enable).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(hocon_schema).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    parse_server/1
]).

-export([
    connector_examples/1
]).

%% `emqx_schema_hooks' API
-export([injected_fields/0]).

-export([
    unique_static_clientid_validator/1
]).

-define(CONNECTOR_TYPE, mqtt).
-define(MQTT_HOST_OPTS, #{default_port => 1883}).

namespace() -> "connector_mqtt".

roots() ->
    fields("config").

fields("config") ->
    fields("server_configs") ++
        [
            {"ingress",
                mk(
                    ref(?MODULE, "ingress"),
                    #{
                        required => {false, recursively},
                        desc => ?DESC("ingress_desc")
                    }
                )},
            {"egress",
                mk(
                    ref(?MODULE, "egress"),
                    #{
                        required => {false, recursively},
                        desc => ?DESC("egress_desc")
                    }
                )}
        ];
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields("specific_connector_config");
fields("specific_connector_config") ->
    [{pool_size, fun egress_pool_size/1}] ++
        emqx_connector_schema:resource_opts_ref(?MODULE, resource_opts) ++
        fields("server_configs");
fields(resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("server_configs") ->
    [
        {mode,
            mk(
                hoconsc:enum([cluster_shareload]),
                #{
                    default => cluster_shareload,
                    desc => ?DESC("mode"),
                    deprecated => {since, "v5.1.0 & e5.1.0"}
                }
            )},
        {server, emqx_schema:servers_sc(#{desc => ?DESC("server")}, ?MQTT_HOST_OPTS)},
        {clientid_prefix, mk(binary(), #{required => false, desc => ?DESC("clientid_prefix")})},
        {static_clientids,
            mk(
                hoconsc:array(ref(?MODULE, static_clientid_entry)),
                #{
                    desc => ?DESC("static_clientid_entry"),
                    default => [],
                    converter => fun static_clientid_converter/2,
                    validator => fun static_clientid_validator/1
                }
            )},
        {reconnect_interval, mk(string(), #{deprecated => {since, "v5.0.16"}})},
        {proto_ver,
            mk(
                hoconsc:enum([v3, v4, v5]),
                #{
                    default => v4,
                    desc => ?DESC("proto_ver")
                }
            )},
        {bridge_mode,
            mk(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC("bridge_mode")
                }
            )},
        {username,
            mk(
                binary(),
                #{
                    desc => ?DESC("username")
                }
            )},
        {password,
            emqx_schema_secret:mk(
                #{
                    desc => ?DESC("password")
                }
            )},
        {clean_start,
            mk(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("clean_start")
                }
            )},
        {keepalive, mk_duration("MQTT Keepalive.", #{default => <<"160s">>})},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_s(), #{
                default => <<"10s">>, desc => ?DESC("connect_timeout")
            })},
        {retry_interval,
            mk_duration(
                "Message retry interval. Delay for the MQTT bridge to retry sending the QoS1/QoS2 "
                "messages in case of ACK not received.",
                #{default => <<"15s">>}
            )},
        {max_inflight,
            mk(
                non_neg_integer(),
                #{
                    default => 32,
                    desc => ?DESC("max_inflight")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("ingress") ->
    [
        {pool_size, fun ingress_pool_size/1},
        %% array
        {remote,
            mk(
                ref(?MODULE, "ingress_remote"),
                #{desc => ?DESC("ingress_remote")}
            )},
        {local,
            mk(
                ref(?MODULE, "ingress_local"),
                #{
                    desc => ?DESC("ingress_local")
                }
            )}
    ];
fields(connector_ingress) ->
    [
        {remote,
            mk(
                ref(?MODULE, "ingress_remote"),
                #{desc => ?DESC("ingress_remote")}
            )},
        {local,
            mk(
                ref(?MODULE, "ingress_local"),
                #{
                    desc => ?DESC("ingress_local"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields("ingress_remote") ->
    %% Avoid modifying this field, as it's used by bridge v1 API/schema.
    [
        {topic,
            mk(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_schema:non_empty_string/1,
                    desc => ?DESC("ingress_remote_topic")
                }
            )},
        {qos,
            mk(
                emqx_schema:qos(),
                #{
                    default => 1,
                    desc => ?DESC("ingress_remote_qos")
                }
            )}
    ];
fields("ingress_local") ->
    %% Avoid modifying this field, as it's used by bridge v1 API/schema.
    [
        {topic,
            mk(
                emqx_schema:template(),
                #{
                    validator => fun emqx_schema:non_empty_string/1,
                    desc => ?DESC("ingress_local_topic"),
                    required => false
                }
            )},
        {qos,
            mk(
                qos(),
                #{
                    default => <<"${qos}">>,
                    desc => ?DESC("ingress_local_qos")
                }
            )},
        {retain,
            mk(
                hoconsc:union([boolean(), emqx_schema:template()]),
                #{
                    default => <<"${retain}">>,
                    desc => ?DESC("retain")
                }
            )},
        {payload,
            mk(
                emqx_schema:template(),
                #{
                    default => undefined,
                    desc => ?DESC("payload")
                }
            )}
    ];
fields("egress") ->
    [
        {pool_size, fun egress_pool_size/1},
        {local,
            mk(
                ref(?MODULE, "egress_local"),
                #{
                    desc => ?DESC("egress_local"),
                    required => false
                }
            )},
        {remote,
            mk(
                ref(?MODULE, "egress_remote"),
                #{
                    desc => ?DESC("egress_remote"),
                    required => true
                }
            )}
    ];
fields("egress_local") ->
    %% Avoid modifying this field, as it's used by bridge v1 API/schema.
    [
        {topic,
            mk(
                binary(),
                #{
                    desc => ?DESC("egress_local_topic"),
                    required => false,
                    validator => fun emqx_schema:non_empty_string/1
                }
            )}
    ];
fields("egress_remote") ->
    %% Avoid modifying this field, as it's used by bridge v1 API/schema.
    [
        {topic,
            mk(
                emqx_schema:template(),
                #{
                    required => true,
                    validator => fun emqx_schema:non_empty_string/1,
                    desc => ?DESC("egress_remote_topic")
                }
            )},
        {qos,
            mk(
                qos(),
                #{
                    required => false,
                    default => 1,
                    desc => ?DESC("egress_remote_qos")
                }
            )},
        {retain,
            mk(
                hoconsc:union([boolean(), emqx_schema:template()]),
                #{
                    required => false,
                    default => false,
                    desc => ?DESC("retain")
                }
            )},
        {payload,
            mk(
                emqx_schema:template(),
                #{
                    default => undefined,
                    desc => ?DESC("payload")
                }
            )}
    ];
fields(static_clientid_entry) ->
    [
        {node,
            mk(
                binary(),
                #{desc => ?DESC("static_clientid_entry_node"), required => true}
            )},
        {ids,
            mk(
                hoconsc:array(hoconsc:union([binary(), ref(?MODULE, static_clientid_entry_tuple)])),
                #{
                    desc => ?DESC("static_clientid_entry_ids"),
                    required => true,
                    validator => fun static_clientid_validate_clientids_length/1
                }
            )}
    ];
fields(static_clientid_entry_tuple) ->
    [
        {clientid,
            mk(binary(), #{required => true, desc => ?DESC("static_clientid_entry_clientid")})},
        {username,
            mk(binary(), #{required => false, desc => ?DESC("static_clientid_entry_username")})},
        {password, emqx_schema_secret:mk(#{required => false, desc => ?DESC("password")})}
    ];
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields = fields("specific_connector_config"),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(What) ->
    error({?MODULE, missing_field_handler, What}).

ingress_pool_size(desc) ->
    ?DESC("ingress_pool_size");
ingress_pool_size(Prop) ->
    emqx_connector_schema_lib:pool_size(Prop).

egress_pool_size(desc) ->
    ?DESC("egress_pool_size");
egress_pool_size(Prop) ->
    emqx_connector_schema_lib:pool_size(Prop).

desc("server_configs") ->
    ?DESC("server_configs");
desc("config_connector") ->
    ?DESC("config_connector");
desc("ingress") ->
    ?DESC("ingress_desc");
desc("ingress_remote") ->
    ?DESC("ingress_remote");
desc("ingress_local") ->
    ?DESC("ingress_local");
desc("egress") ->
    ?DESC("egress_desc");
desc("egress_remote") ->
    ?DESC("egress_remote");
desc("egress_local") ->
    ?DESC("egress_local");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, <<"resource_opts">>);
desc(static_clientid_entry) ->
    ?DESC("static_clientid_entry");
desc(static_clientid_entry_tuple) ->
    ?DESC("static_clientid_entry");
desc(_) ->
    undefined.

injected_fields() ->
    #{
        'connectors.validators' => [fun ?MODULE:unique_static_clientid_validator/1]
    }.

unique_static_clientid_validator(#{<<"mqtt">> := MQTTConnectors}) ->
    StaticClientIdsToNames0 =
        [
            {{ClientId, Server}, Name}
         || {Name, #{<<"static_clientids">> := CIdMappings, <<"server">> := Server}} <-
                maps:to_list(MQTTConnectors),
            #{<<"ids">> := ClientIds} <- CIdMappings,
            #{<<"clientid">> := ClientId} <- ClientIds
        ],
    StaticClientIdsToNames =
        maps:groups_from_list(
            fun({CIdServer, _Name}) -> CIdServer end,
            fun({_CIdServer, Name}) -> Name end,
            StaticClientIdsToNames0
        ),
    Duplicated0 = maps:filter(fun(_, Vs) -> length(Vs) > 1 end, StaticClientIdsToNames),
    Duplicated = maps:values(Duplicated0),
    case Duplicated of
        [] ->
            ok;
        [_ | _] ->
            DuplicatedFormatted = format_duplicated_name_groups(Duplicated),
            Msg =
                iolist_to_binary(
                    io_lib:format(
                        "distinct mqtt connectors must not use the same static clientids;"
                        " connectors with duplicate static clientids: ~s",
                        [DuplicatedFormatted]
                    )
                ),
            {error, Msg}
    end;
unique_static_clientid_validator(_) ->
    ok.

qos() ->
    hoconsc:union([emqx_schema:qos(), emqx_schema:template()]).

parse_server(Str) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Str, ?MQTT_HOST_OPTS),
    {Host, Port}.

connector_examples(Method) ->
    [
        #{
            <<"mqtt">> => #{
                summary => <<"MQTT Connector">>,
                value => connector_example(Method)
            }
        }
    ].

connector_example(get) ->
    maps:merge(
        connector_example(put),
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
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => atom_to_binary(?CONNECTOR_TYPE),
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My connector">>,
        pool_size => 3,
        proto_ver => <<"v5">>,
        server => <<"127.0.0.1:1883">>,
        resource_opts => #{
            health_check_interval => <<"45s">>,
            start_after_created => true,
            start_timeout => <<"5s">>
        }
    }.

static_clientid_converter(undefined, _HoconOpts) ->
    undefined;
static_clientid_converter(Entries, _HoconOpts) ->
    lists:map(fun static_clientid_converter1/1, Entries).

static_clientid_converter1(#{node := _} = Entry0) ->
    Entry = emqx_utils_maps:binary_key_map(Entry0),
    static_clientid_converter1(Entry);
static_clientid_converter1(#{<<"ids">> := Ids0} = Entry0) ->
    Ids =
        lists:map(
            fun
                (Id) when is_binary(Id) ->
                    #{<<"clientid">> => Id};
                (M) when is_map(M) ->
                    M
            end,
            Ids0
        ),
    Entry0#{<<"ids">> := Ids}.

static_clientid_validator([]) ->
    ok;
static_clientid_validator([#{node := _} | _] = Entries0) ->
    Entries = lists:map(fun emqx_utils_maps:binary_key_map/1, Entries0),
    static_clientid_validator(Entries);
static_clientid_validator([_ | _] = Entries) ->
    maybe
        ok ?= static_clientid_validate_distinct_nodes(Entries),
        ok ?= static_clientid_validate_at_least_one_clientid(Entries),
        ok ?= static_clientid_validate_distinct_clientids(Entries)
    end.

static_clientid_validate_at_least_one_clientid(Entries) ->
    AllIds = lists:flatmap(fun(#{<<"ids">> := Ids}) -> Ids end, Entries),
    case AllIds of
        [] ->
            {error, <<"must specify at least one static clientid">>};
        [_ | _] ->
            ok
    end.

static_clientid_validate_distinct_nodes(Entries) ->
    AllNodes = lists:map(fun(#{<<"node">> := Node}) -> Node end, Entries),
    UniqueNodes = lists:uniq(AllNodes),
    case AllNodes -- UniqueNodes of
        [] ->
            ok;
        [_ | _] = DuplicatedNodes0 ->
            DuplicatedNodes = lists:join(<<", ">>, lists:usort(DuplicatedNodes0)),
            Msg = iolist_to_binary([
                <<"nodes must be unique; duplicated nodes: ">>,
                DuplicatedNodes
            ]),
            {error, Msg}
    end.

static_clientid_validate_distinct_clientids(Entries) ->
    AllIds = lists:flatmap(
        fun(#{<<"ids">> := Ids}) ->
            lists:map(fun(#{<<"clientid">> := ClientId}) -> ClientId end, Ids)
        end,
        Entries
    ),
    UniqueIds = lists:uniq(AllIds),
    case AllIds -- UniqueIds of
        [] ->
            ok;
        [_ | _] = DuplicatedIds0 ->
            DuplicatedIds = lists:join(<<", ">>, lists:usort(DuplicatedIds0)),

            Msg = iolist_to_binary([
                <<"clientids must be unique; duplicated clientids: ">>,
                DuplicatedIds
            ]),
            {error, Msg}
    end.

static_clientid_validate_clientids_length(Ids0) ->
    Ids = lists:map(fun emqx_utils_maps:binary_key_map/1, Ids0),
    case lists:any(fun(#{<<"clientid">> := Id}) -> Id == <<"">> end, Ids) of
        true ->
            {error, <<"clientids must be non-empty">>};
        false ->
            ok
    end.

mk_duration(Desc, Opts) ->
    emqx_schema:mk_duration(Desc, Opts).

mk(Type, Opts) ->
    hoconsc:mk(Type, Opts).

ref(SchemaModule, StructName) ->
    hoconsc:ref(SchemaModule, StructName).

format_duplicated_name_groups(DuplicatedNameGroups) ->
    lists:join(
        $;,
        lists:map(
            fun(NameGroup) ->
                lists:join($,, lists:sort(NameGroup))
            end,
            DuplicatedNameGroups
        )
    ).
