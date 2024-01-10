%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_es).

-include("emqx_bridge_es.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([bridge_v2_examples/1]).

%% hocon_schema API
-export([namespace/0, roots/0, fields/1, desc/1]).

-define(CONNECTOR_TYPE, elasticsearch).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

namespace() -> "bridge_elasticsearch".

roots() -> [].

fields(action) ->
    {elasticsearch,
        ?HOCON(
            ?MAP(action_name, ?R_REF(action_config)),
            #{
                desc => <<"ElasticSearch Action Config">>,
                required => false
            }
        )};
fields(action_config) ->
    emqx_resource_schema:override(
        emqx_bridge_v2_schema:make_producer_action_schema(
            ?HOCON(
                ?R_REF(action_parameters),
                #{
                    required => true, desc => ?DESC("action_parameters")
                }
            )
        ),
        [
            {resource_opts,
                ?HOCON(?R_REF(action_resource_opts), #{
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "resource_opts")
                })}
        ]
    );
fields(action_resource_opts) ->
    lists:filter(
        fun({K, _V}) ->
            not lists:member(K, unsupported_opts())
        end,
        emqx_bridge_v2_schema:resource_opts_fields()
    );
fields(action_parameters) ->
    [
        {target,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC("config_target"),
                    required => false
                }
            )},
        {require_alias,
            ?HOCON(
                boolean(),
                #{
                    required => false,
                    default => false,
                    desc => ?DESC("config_require_alias")
                }
            )},
        {routing,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_routing")
                }
            )},
        {wait_for_active_shards,
            ?HOCON(
                ?UNION([pos_integer(), all]),
                #{
                    required => false,
                    desc => ?DESC("config_wait_for_active_shards")
                }
            )},
        {data,
            ?HOCON(
                ?ARRAY(
                    ?UNION(
                        [
                            ?R_REF(create),
                            ?R_REF(delete),
                            ?R_REF(index),
                            ?R_REF(update)
                        ]
                    )
                ),
                #{
                    desc => ?DESC("action_parameters_data")
                }
            )}
    ] ++
        lists:filter(
            fun({K, _}) ->
                not lists:member(K, [path, method, body, headers, request_timeout])
            end,
            emqx_bridge_http_schema:fields("parameters_opts")
        );
fields(Action) when Action =:= create; Action =:= index ->
    [
        {action,
            ?HOCON(
                Action,
                #{
                    desc => atom_to_binary(Action),
                    required => true
                }
            )},
        {'_index',
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_index")
                }
            )},
        {'_id',
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_id")
                }
            )},
        {require_alias,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_require_alias")
                }
            )},
        {fields,
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_fields")
                }
            )}
    ];
fields(delete) ->
    [
        {action,
            ?HOCON(
                delete,
                #{
                    desc => <<"Delete">>,
                    required => true
                }
            )},
        {'_index',
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_index")
                }
            )},
        {'_id',
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_id")
                }
            )},
        {require_alias,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_require_alias")
                }
            )}
    ];
fields(update) ->
    [
        {action,
            ?HOCON(
                update,
                #{
                    desc => <<"Update">>,
                    required => true
                }
            )},
        {doc_as_upsert,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_doc_as_upsert")
                }
            )},
        {upsert,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_upsert")
                }
            )},
        {'_index',
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_index")
                }
            )},
        {'_id',
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_id")
                }
            )},
        {require_alias,
            ?HOCON(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_parameters_require_alias")
                }
            )},
        {fields,
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_fields")
                }
            )}
    ];
fields("post_bridge_v2") ->
    emqx_bridge_schema:type_and_name_fields(elasticsearch) ++ fields(action_config);
fields("put_bridge_v2") ->
    fields(action_config);
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2").

bridge_v2_examples(Method) ->
    [
        #{
            <<"elasticsearch">> =>
                #{
                    summary => <<"Elastic Search Bridge">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        parameters => #{
            target => <<"${target_index}">>,
            data => [
                #{
                    action => index,
                    '_index' => <<"${index}">>,
                    fields => <<"${fields}">>,
                    require_alias => <<"${require_alias}">>
                },
                #{
                    action => create,
                    '_index' => <<"${index}">>,
                    fields => <<"${fields}">>
                },
                #{
                    action => delete,
                    '_index' => <<"${index}">>,
                    '_id' => <<"${id}">>
                },
                #{
                    action => update,
                    '_index' => <<"${index}">>,
                    '_id' => <<"${id}">>,
                    fields => <<"${fields}">>,
                    require_alias => false,
                    doc_as_upsert => <<"${doc_as_upsert}">>,
                    upsert => <<"${upsert}">>
                }
            ]
        }
    }.

unsupported_opts() ->
    [
        batch_size,
        batch_time
    ].

desc(_) -> undefined.
