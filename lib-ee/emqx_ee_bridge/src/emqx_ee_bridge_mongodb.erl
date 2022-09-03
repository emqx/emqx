%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_mongodb).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(hocon_schema).

%% emqx_ee_bridge "callbacks"
-export([
    conn_bridge_examples/1
]).

%% hocon_schema callbacks
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%%=================================================================================================
%% hocon_schema API
%%=================================================================================================

namespace() ->
    "bridge_mongodb".

roots() ->
    [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("enable"), default => true})},
        {collection, mk(binary(), #{desc => ?DESC("collection"), default => <<"mqtt">>})}
    ];
fields(mongodb_rs) ->
    emqx_connector_mongo:fields(rs) ++ fields("config");
fields(mongodb_sharded) ->
    emqx_connector_mongo:fields(sharded) ++ fields("config");
fields(mongodb_single) ->
    emqx_connector_mongo:fields(single) ++ fields("config");
fields("post_rs") ->
    fields(mongodb_rs);
fields("post_sharded") ->
    fields(mongodb_sharded);
fields("post_single") ->
    fields(mongodb_single);
fields("put_rs") ->
    fields(mongodb_rs);
fields("put_sharded") ->
    fields(mongodb_sharded);
fields("put_single") ->
    fields(mongodb_single);
fields("get_rs") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields(mongodb_rs);
fields("get_sharded") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields(mongodb_sharded);
fields("get_single") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields(mongodb_single).

conn_bridge_examples(Method) ->
    [
        #{
            <<"mongodb_rs">> => #{
                summary => <<"MongoDB (Replica Set) Bridge">>,
                value => values(mongodb_rs, Method)
            }
        },
        #{
            <<"mongodb_sharded">> => #{
                summary => <<"MongoDB (Sharded) Bridge">>,
                value => values(mongodb_sharded, Method)
            }
        },
        #{
            <<"mongodb_single">> => #{
                summary => <<"MongoDB (Standalone) Bridge">>,
                value => values(mongodb_single, Method)
            }
        }
    ].

desc("config") ->
    ?DESC("desc_config");
desc(mongodb_rs) ->
    ?DESC(mongodb_rs_conf);
desc(mongodb_sharded) ->
    ?DESC(mongodb_sharded_conf);
desc(mongodb_single) ->
    ?DESC(mongodb_single_conf);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for MongoDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%%=================================================================================================
%% Internal fns
%%=================================================================================================

values(mongodb_rs = MongoType, Method) ->
    TypeOpts = #{
        servers => <<"localhost:27017, localhost:27018">>,
        w_mode => <<"safe">>,
        r_mode => <<"safe">>,
        replica_set_name => <<"rs">>
    },
    values(common, MongoType, Method, TypeOpts);
values(mongodb_sharded = MongoType, Method) ->
    TypeOpts = #{
        servers => <<"localhost:27017, localhost:27018">>,
        w_mode => <<"safe">>
    },
    values(common, MongoType, Method, TypeOpts);
values(mongodb_single = MongoType, Method) ->
    TypeOpts = #{
        server => <<"localhost:27017">>,
        w_mode => <<"safe">>
    },
    values(common, MongoType, Method, TypeOpts).

values(common, MongoType, Method, TypeOpts) ->
    MongoTypeBin = atom_to_binary(MongoType),
    Common = #{
        name => <<MongoTypeBin/binary, "_demo">>,
        type => MongoTypeBin,
        enable => true,
        collection => <<"mycol">>,
        database => <<"mqtt">>,
        srv_record => false,
        pool_size => 8,
        username => <<"myuser">>,
        password => <<"mypass">>
    },
    MethodVals = method_values(MongoType, Method),
    Vals0 = maps:merge(MethodVals, Common),
    maps:merge(Vals0, TypeOpts).

method_values(MongoType, get) ->
    Vals = method_values(MongoType, post),
    maps:merge(?METRICS_EXAMPLE, Vals);
method_values(MongoType, _) ->
    ConnectorType =
        case MongoType of
            mongodb_rs -> <<"rs">>;
            mongodb_sharded -> <<"sharded">>;
            mongodb_single -> <<"single">>
        end,
    #{mongo_type => ConnectorType}.
