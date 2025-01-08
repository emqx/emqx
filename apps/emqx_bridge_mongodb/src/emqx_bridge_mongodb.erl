%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mongodb).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(hocon_schema).

%% emqx_bridge_enterprise "callbacks"
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

%% hocon_schema callbacks
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, mongodb).
-define(ACTION_TYPE, mongodb).

%%=================================================================================================
%% hocon_schema API
%%=================================================================================================

%% [TODO] Namespace should be different depending on whether this is used for a
%% connector, an action or a legacy bridge type.
namespace() ->
    "bridge_mongodb".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("enable"), default => true})},
        {collection,
            mk(emqx_schema:template(), #{desc => ?DESC("collection"), default => <<"mqtt">>})},
        {payload_template,
            mk(emqx_schema:template(), #{required => false, desc => ?DESC("payload_template")})},
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{required => true, desc => ?DESC(emqx_resource_schema, "creation_opts")}
            )}
    ];
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields("connection_fields") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields("connection_fields") ->
    [
        {parameters,
            mk(
                hoconsc:union([
                    ref(emqx_mongodb, "connector_" ++ T)
                 || T <- ["single", "sharded", "rs"]
                ]),
                #{required => true, desc => ?DESC("mongodb_parameters")}
            )}
    ] ++ emqx_mongodb:fields(mongodb);
fields("creation_opts") ->
    %% so far, mongodb connector does not support batching
    %% but we cannot delete this field due to compatibility reasons
    %% so we'll keep this field, but hide it in the docs.
    emqx_resource_schema:create_opts([
        {batch_size, #{
            importance => ?IMPORTANCE_HIDDEN,
            converter => fun(_, _) -> 1 end,
            desc => ?DESC("batch_size")
        }},
        {batch_time, #{
            importance => ?IMPORTANCE_HIDDEN,
            converter => fun(_, _) -> 0 end,
            desc => ?DESC("batch_size")
        }}
    ]);
fields(action) ->
    {mongodb,
        mk(
            hoconsc:map(name, ref(?MODULE, mongodb_action)),
            #{desc => <<"MongoDB Action Config">>, required => false}
        )};
fields(mongodb_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        }),
        #{resource_opts_ref => ref(?MODULE, action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {collection,
            mk(emqx_schema:template(), #{desc => ?DESC("collection"), default => <<"mqtt">>})},
        {payload_template,
            mk(emqx_schema:template(), #{required => false, desc => ?DESC("payload_template")})}
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{
            importance => ?IMPORTANCE_HIDDEN,
            converter => fun(_, _) -> 1 end,
            desc => ?DESC("batch_size")
        }},
        {batch_time, #{
            importance => ?IMPORTANCE_HIDDEN,
            converter => fun(_, _) -> 0 end,
            desc => ?DESC("batch_size")
        }}
    ]);
fields(resource_opts) ->
    fields("creation_opts");
fields(mongodb_rs) ->
    emqx_mongodb:fields(rs) ++ fields("config");
fields(mongodb_sharded) ->
    emqx_mongodb:fields(sharded) ++ fields("config");
fields(mongodb_single) ->
    emqx_mongodb:fields(single) ++ fields("config");
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields("connection_fields") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(mongodb_action));
fields("post_rs") ->
    fields(mongodb_rs) ++ emqx_bridge_schema:type_and_name_fields(mongodb_rs);
fields("post_sharded") ->
    fields(mongodb_sharded) ++ emqx_bridge_schema:type_and_name_fields(mongodb_sharded);
fields("post_single") ->
    fields(mongodb_single) ++ emqx_bridge_schema:type_and_name_fields(mongodb_single);
fields("put_rs") ->
    fields(mongodb_rs);
fields("put_sharded") ->
    fields(mongodb_sharded);
fields("put_single") ->
    fields(mongodb_single);
fields("get_rs") ->
    emqx_bridge_schema:status_fields() ++
        fields(mongodb_rs) ++
        emqx_bridge_schema:type_and_name_fields(mongodb_rs);
fields("get_sharded") ->
    emqx_bridge_schema:status_fields() ++
        fields(mongodb_sharded) ++
        emqx_bridge_schema:type_and_name_fields(mongodb_sharded);
fields("get_single") ->
    emqx_bridge_schema:status_fields() ++
        fields(mongodb_single) ++
        emqx_bridge_schema:type_and_name_fields(mongodb_single).

bridge_v2_examples(Method) ->
    [
        #{
            <<"mongodb">> => #{
                summary => <<"MongoDB Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, mongodb, mongodb, #{parameters => #{collection => <<"mycol">>}}
                )
            }
        }
    ].

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

connector_examples(Method) ->
    [
        #{
            <<"mongodb_rs">> => #{
                summary => <<"MongoDB Replica Set Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, mongodb_rs, #{parameters => connector_values()}
                )
            }
        },
        #{
            <<"mongodb_sharded">> => #{
                summary => <<"MongoDB Sharded Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, mongodb_sharded, #{parameters => connector_values()}
                )
            }
        },
        #{
            <<"mongodb_single">> => #{
                summary => <<"MongoDB Standalone Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, mongodb_single, #{parameters => connector_values()}
                )
            }
        }
    ].

desc("config_connector") ->
    ?DESC("desc_config");
desc("config") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(mongodb_rs) ->
    ?DESC(mongodb_rs_conf);
desc(mongodb_sharded) ->
    ?DESC(mongodb_sharded_conf);
desc(mongodb_single) ->
    ?DESC(mongodb_single_conf);
desc(mongodb_action) ->
    ?DESC(mongodb_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for MongoDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%%=================================================================================================
%% Internal fns
%%=================================================================================================

values(MongoType, Method) ->
    maps:merge(
        mongo_type_opts(MongoType),
        bridge_values(MongoType, Method)
    ).

mongo_type_opts(mongodb_rs) ->
    #{
        mongo_type => <<"rs">>,
        servers => <<"localhost:27017, localhost:27018">>,
        w_mode => <<"safe">>,
        r_mode => <<"safe">>,
        replica_set_name => <<"rs">>
    };
mongo_type_opts(mongodb_sharded) ->
    #{
        mongo_type => <<"sharded">>,
        servers => <<"localhost:27017, localhost:27018">>,
        w_mode => <<"safe">>
    };
mongo_type_opts(mongodb_single) ->
    #{
        mongo_type => <<"single">>,
        server => <<"localhost:27017">>,
        w_mode => <<"safe">>
    }.

bridge_values(Type, _Method) ->
    %% [FIXME] _Method makes a difference since PUT doesn't allow name and type
    %% for connectors.
    TypeBin = atom_to_binary(Type),
    maps:merge(
        #{
            name => <<TypeBin/binary, "_demo">>,
            type => TypeBin,
            collection => <<"mycol">>
        },
        connector_values()
    ).

connector_values() ->
    #{
        enable => true,
        database => <<"mqtt">>,
        srv_record => false,
        pool_size => 8,
        username => <<"myuser">>,
        password => <<"******">>
    }.
