%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-define(TYPE, redis).

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    resource_opts_converter/2
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    ?TYPE.

roots() ->
    [].

%%=========================================
%% Action fields
%%=========================================
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        [
            {parameters,
                ?HOCON(
                    hoconsc:union([
                        ?R_REF(emqx_redis, redis_single_connector),
                        ?R_REF(emqx_redis, redis_sentinel_connector),
                        ?R_REF(emqx_redis, redis_cluster_connector)
                    ]),
                    #{required => true, desc => ?DESC(redis_parameters)}
                )}
        ] ++
        emqx_redis:redis_fields() ++
        emqx_connector_schema_lib:ssl_fields();
fields(action) ->
    {?TYPE,
        ?HOCON(
            ?MAP(name, ?R_REF(redis_action)),
            #{
                desc => <<"Redis Action Config">>,
                converter => fun ?MODULE:resource_opts_converter/2,
                required => false
            }
        )};
fields(redis_action) ->
    Schema =
        emqx_bridge_v2_schema:make_producer_action_schema(
            ?HOCON(
                ?R_REF(emqx_bridge_redis, action_parameters),
                #{
                    required => true,
                    desc => ?DESC(producer_action)
                }
            )
        ),
    ResOpts =
        {resource_opts,
            ?HOCON(
                ?R_REF(resource_opts),
                #{
                    required => true,
                    desc => ?DESC(emqx_resource_schema, resource_opts)
                }
            )},
    RedisType =
        {redis_type,
            ?HOCON(
                ?ENUM([single, sentinel, cluster]),
                #{required => true, desc => ?DESC(redis_type)}
            )},
    [RedisType | lists:keyreplace(resource_opts, 1, Schema, ResOpts)];
fields(resource_opts) ->
    emqx_resource_schema:create_opts([
        {batch_size, #{desc => ?DESC(batch_size)}},
        {batch_time, #{desc => ?DESC(batch_time)}}
    ]);
%%=========================================
%% HTTP API fields
%%=========================================
fields("post_connector") ->
    emqx_bridge_redis:type_name_fields(?TYPE) ++ fields("config_connector");
fields("put_connector") ->
    fields("config_connector");
fields("get_connector") ->
    emqx_bridge_schema:status_fields() ++
        fields("post_connector");
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2");
fields("post_bridge_v2") ->
    emqx_bridge_redis:type_name_fields(?TYPE) ++ fields("put_bridge_v2");
fields("put_bridge_v2") ->
    fields(redis_action);
fields("get_single") ->
    emqx_bridge_schema:status_fields() ++ fields("put_single");
fields("put_single") ->
    fields("config_connector");
fields("post_single") ->
    emqx_bridge_redis:type_name_fields(?TYPE) ++ fields("put_single").

desc("config_connector") ->
    ?DESC(emqx_bridge_redis, "desc_config");
desc(redis_action) ->
    ?DESC(redis_action);
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

resource_opts_converter(undefined, _Opts) ->
    undefined;
resource_opts_converter(Conf, _Opts) ->
    maps:map(
        fun(_Name, SubConf) ->
            case SubConf of
                #{<<"redis_type">> := <<"cluster">>} ->
                    ResOpts = maps:get(<<"resource_opts">>, SubConf, #{}),
                    %% cluster don't support batch
                    SubConf#{
                        <<"resource_opts">> =>
                            ResOpts#{<<"batch_size">> => 1, <<"batch_time">> => <<"0ms">>}
                    };
                _ ->
                    SubConf
            end
        end,
        Conf
    ).

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"redis_single_producer">> => #{
                summary => <<"Redis Single Producer Action">>,
                value => action_example(single, Method)
            }
        },
        #{
            <<"redis_sentinel_producer">> => #{
                summary => <<"Redis Sentinel Producer Action">>,
                value => action_example(sentinel, Method)
            }
        },
        #{
            <<"redis_cluster_producer">> => #{
                summary => <<"Redis Cluster Producer Action">>,
                value => action_example(cluster, Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"redis_single_producer">> => #{
                summary => <<"Redis Single Producer Connector">>,
                value => connector_example(single, Method)
            }
        },
        #{
            <<"redis_cluster_producer">> => #{
                summary => <<"Redis Cluster Producer Connector">>,
                value => connector_example(cluster, Method)
            }
        },
        #{
            <<"redis_sentinel_producer">> => #{
                summary => <<"Redis Sentinel Producer Connector">>,
                value => connector_example(sentinel, Method)
            }
        }
    ].

conn_bridge_examples(Method) ->
    emqx_bridge_redis:conn_bridge_examples(Method).

action_example(RedisType, post) ->
    maps:merge(
        action_example(RedisType, put),
        #{
            type => <<"redis">>,
            name => <<"my_action">>
        }
    );
action_example(RedisType, get) ->
    maps:merge(
        action_example(RedisType, put),
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
action_example(RedisType, put) ->
    #{
        redis_type => RedisType,
        enable => true,
        connector => <<"my_connector_name">>,
        description => <<"My action">>,
        parameters => #{
            command_template => [<<"LPUSH">>, <<"MSGS">>, <<"${payload}">>]
        },
        resource_opts => #{batch_size => 1}
    }.

connector_example(RedisType, get) ->
    maps:merge(
        connector_example(RedisType, put),
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
connector_example(RedisType, post) ->
    maps:merge(
        connector_example(RedisType, put),
        #{
            type => <<"redis_single_producer">>,
            name => <<"my_connector">>
        }
    );
connector_example(RedisType, put) ->
    #{
        enable => true,
        desc => <<"My redis ", (atom_to_binary(RedisType))/binary, " connector">>,
        parameters => connector_parameter(RedisType),
        pool_size => 8,
        database => 1,
        username => <<"test">>,
        password => <<"******">>,
        auto_reconnect => true,
        ssl => #{enable => false}
    }.

connector_parameter(single) ->
    #{redis_type => single, server => <<"127.0.0.1:6379">>};
connector_parameter(cluster) ->
    #{redis_type => cluster, servers => <<"127.0.0.1:6379,127.0.0.2:6379">>};
connector_parameter(sentinel) ->
    #{
        redis_type => sentinel,
        servers => <<"127.0.0.1:6379,127.0.0.2:6379">>,
        sentinel => <<"myredismaster">>
    }.
