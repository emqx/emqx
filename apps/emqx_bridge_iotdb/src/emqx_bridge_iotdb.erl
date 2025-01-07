%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb).

-include("emqx_bridge_iotdb.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2, array/1]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1
]).

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, iotdb).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "bridge_iotdb".

roots() -> [].

%%-------------------------------------------------------------------------------------------------
%% v2 schema
%%-------------------------------------------------------------------------------------------------

fields(action) ->
    {iotdb,
        mk(
            hoconsc:map(name, ref(?MODULE, action_config)),
            #{
                desc => <<"IoTDB Action Config">>,
                required => false
            }
        )};
fields(action_config) ->
    emqx_resource_schema:override(
        emqx_bridge_v2_schema:make_producer_action_schema(
            mk(
                ref(?MODULE, action_parameters),
                #{
                    required => true, desc => ?DESC("action_parameters")
                }
            )
        ),
        [
            {resource_opts,
                mk(ref(?MODULE, action_resource_opts), #{
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "resource_opts")
                })}
        ]
    );
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields();
fields(action_parameters) ->
    [
        {is_aligned,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_is_aligned"),
                    default => false
                }
            )},
        {device_id,
            mk(
                emqx_schema:template(),
                #{
                    desc => ?DESC("config_device_id")
                }
            )},
        {data,
            mk(
                array(ref(?MODULE, action_parameters_data)),
                #{
                    desc => ?DESC("action_parameters_data"),
                    required => true,
                    validator => fun emqx_schema:non_empty_array/1
                }
            )}
    ] ++
        proplists_without(
            [path, method, body, headers, request_timeout],
            emqx_bridge_http_schema:fields("parameters_opts")
        );
fields(action_parameters_data) ->
    [
        {timestamp,
            mk(
                hoconsc:union([enum([now, now_ms, now_ns, now_us]), emqx_schema:template()]),
                #{
                    desc => ?DESC("config_parameters_timestamp"),
                    default => <<"now">>
                }
            )},
        {measurement,
            mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_measurement")
                }
            )},
        {data_type,
            mk(
                enum([text, boolean, int32, int64, float, double]),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_data_type")
                }
            )},
        {value,
            mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_value")
                }
            )}
    ];
fields("post_bridge_v2") ->
    emqx_bridge_schema:type_and_name_fields(enum([iotdb])) ++ fields(action_config);
fields("put_bridge_v2") ->
    fields(action_config);
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2");
%%-------------------------------------------------------------------------------------------------
%% v1 schema
%%-------------------------------------------------------------------------------------------------

fields("config") ->
    basic_config() ++ request_config();
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields(auth_basic) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("config_auth_basic_username")})},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("config_auth_basic_password")
            })}
    ];
fields("post") ->
    emqx_bridge_schema:type_and_name_fields(enum([iotdb])) ++ fields("config");
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(action_config) ->
    ?DESC("desc_config");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(action_parameters_data) ->
    ?DESC("action_parameters_data");
desc(action_resource_opts) ->
    "Action Resource Options";
desc("creation_opts") ->
    "Creation Options";
desc(auth_basic) ->
    "Basic Authentication";
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for IoTDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

basic_config() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_enable"),
                    default => true
                }
            )},
        {authentication,
            mk(
                hoconsc:union([ref(?MODULE, auth_basic)]),
                #{
                    default => auth_basic, desc => ?DESC("config_authentication")
                }
            )},
        {is_aligned,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_is_aligned"),
                    default => false
                }
            )},
        {device_id,
            mk(
                binary(),
                #{
                    desc => ?DESC("config_device_id")
                }
            )},
        {iotdb_version,
            mk(
                hoconsc:enum([?VSN_1_3_X, ?VSN_1_1_X, ?VSN_1_0_X, ?VSN_0_13_X]),
                #{
                    desc => ?DESC("config_iotdb_version"),
                    default => ?VSN_1_3_X
                }
            )}
    ] ++ resource_creation_opts() ++
        proplists_without(
            [max_retries, base_url, request],
            emqx_bridge_http_connector:fields(config)
        ).

proplists_without(Keys, List) ->
    [El || El = {K, _} <- List, not lists:member(K, Keys)].

request_config() ->
    [
        {base_url,
            mk(
                emqx_schema:url(),
                #{
                    required => true,
                    desc => ?DESC("config_base_url")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    default => 2,
                    desc => ?DESC("config_max_retries")
                }
            )}
    ].

resource_creation_opts() ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ].

%%-------------------------------------------------------------------------------------------------
%% v2 examples
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"iotdb">> =>
                #{
                    summary => <<"Apache IoTDB Bridge">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        parameters => #{
            data => [
                #{
                    timestamp => now,
                    measurement => <<"status">>,
                    data_type => <<"BOOLEAN">>,
                    value => <<"${st}">>
                }
            ],
            is_aligned => false,
            device_id => <<"my_device">>
        }
    }.

%%-------------------------------------------------------------------------------------------------
%% v1 examples
%%-------------------------------------------------------------------------------------------------
conn_bridge_examples(Method) ->
    [
        #{
            <<"iotdb">> =>
                #{
                    summary => <<"Apache IoTDB Bridge">>,
                    value => conn_bridge_example(Method, iotdb)
                }
        }
    ].

conn_bridge_example(_Method, Type) ->
    #{
        name => <<"My IoTDB Bridge">>,
        type => Type,
        enable => true,
        authentication => #{
            <<"username">> => <<"root">>,
            <<"password">> => <<"*****">>
        },
        is_aligned => false,
        device_id => <<"my_device">>,
        base_url => <<"http://iotdb.local:18080/">>,
        iotdb_version => ?VSN_1_1_X,
        connect_timeout => <<"15s">>,
        pool_type => <<"random">>,
        pool_size => 8,
        enable_pipelining => 100,
        ssl => #{enable => false},
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.
