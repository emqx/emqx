%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_schema).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-define(TYPE, redis).

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
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts) ++
        emqx_connector_schema_lib:ssl_fields();
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(action) ->
    {?TYPE,
        ?HOCON(
            ?MAP(name, ?R_REF(redis_action)),
            #{
                desc => <<"Redis Action Config">>,
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
    [ResOpts] = emqx_connector_schema:resource_opts_ref(?MODULE, action_resource_opts),
    lists:keyreplace(resource_opts, 1, Schema, ResOpts);
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
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
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"redis">> => #{
                summary => <<"Redis Action">>,
                value => action_example(Method)
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

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => <<"redis">>,
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
            type => <<"redis">>,
            name => <<"my_connector">>
        }
    );
connector_example(RedisType, put) ->
    #{
        enable => true,
        description => <<"My redis ", (atom_to_binary(RedisType))/binary, " connector">>,
        parameters => connector_parameter(RedisType),
        ssl => #{enable => false}
    }.

connector_parameter(single) ->
    #{
        redis_type => single,
        server => <<"127.0.0.1:6379">>,
        pool_size => 8,
        database => 1,
        username => <<"test">>,
        password => <<"******">>
    };
connector_parameter(cluster) ->
    #{
        redis_type => cluster,
        servers => <<"127.0.0.1:6379,127.0.0.2:6379">>,
        pool_size => 8,
        username => <<"test">>,
        password => <<"******">>
    };
connector_parameter(sentinel) ->
    #{
        redis_type => sentinel,
        servers => <<"127.0.0.1:6379,127.0.0.2:6379">>,
        sentinel => <<"myredismaster">>,
        pool_size => 8,
        database => 1,
        username => <<"test">>,
        password => <<"******">>
    }.
