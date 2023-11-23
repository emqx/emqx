%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_sentinel_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-define(TYPE, redis_sentinel).
-define(TYPE_NAME, redis_sentinel_producer).

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
    ?TYPE_NAME.

roots() ->
    [].

%%=========================================
%% Action fields
%%=========================================
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_redis:connector_fields(?TYPE);
fields(action) ->
    {?TYPE_NAME,
        ?HOCON(
            ?MAP(name, ?R_REF(emqx_bridge_redis, "action_redis_sentinel")),
            #{
                desc => <<"Redis Sentinel Producer Action Config">>,
                required => false
            }
        )};
%%=========================================
%% HTTP API fields
%%=========================================
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2");
fields("post_bridge_v2") ->
    emqx_bridge_redis:type_name_fields(?TYPE_NAME) ++ fields("put_bridge_v2");
fields("put_bridge_v2") ->
    emqx_bridge_redis:fields("action_redis_sentinel");
fields("get_sentinel") ->
    emqx_bridge_schema:status_fields() ++ fields("put_sentinel");
fields("put_sentinel") ->
    fields("config_connector");
fields("post_sentinel") ->
    emqx_bridge_redis:type_name_fields(?TYPE_NAME) ++ fields("put_sentinel").

desc("config_connector") ->
    ?DESC(emqx_bridge_redis, "desc_config");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"redis_sentinel_producer">> => #{
                summary => <<"Redis Sentinel Producer Action">>,
                value => action_example(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"redis_sentinel_producer">> => #{
                summary => <<"Redis Sentinel Producer Connector">>,
                value => connector_example(Method)
            }
        }
    ].

conn_bridge_examples(Method) ->
    emqx_bridge_redis:conn_bridge_examples(Method).

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => <<"redis_sentinel_producer">>,
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
        resource_opts => #{batch_size => 5}
    }.

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
            type => <<"redis_sentinel_producer">>,
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        desc => <<"My Redis Sentinel Connector">>,
        server => <<"127.0.0.1:6379">>,
        redis_type => sentinel,
        sentinel => <<"mymaster">>,
        pool_size => 8,
        database => 1,
        username => <<"test">>,
        password => <<"******">>,
        auto_reconnect => true,
        command_template => [
            <<"LPUSH">>,
            <<"MSGS">>,
            <<"${payload}">>
        ],
        ssl => #{enable => false}
    }.
