%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tablestore).

-behaviour(emqx_connector_examples).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-define(CONNECTOR_TYPE, tablestore).
-define(ACTION_TYPE, tablestore).

%% Examples
conn_bridge_examples(Method) ->
    [
        #{
            <<"tablestore_timeseries">> => #{
                summary => <<"Tablestore Timeseries Bridge">>,
                value => values(timeseries, Method)
            }
        }
    ].

bridge_v2_examples(Method) ->
    [
        #{
            <<"tablestore_timeseries">> => #{
                summary => <<"Tablestore Timeseries Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, tablestore, tablestore, action_parameters_example(timeseries)
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"tablestore_timeseries">> => #{
                summary => <<"Tablestore Timeseries Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, tablestore, connector_values(timeseries)
                )
            }
        }
    ].

connector_values(timeseries) ->
    #{
        name => <<"tablestore_connector">>,
        enable => true,
        endpoint => <<"https://myinstance.cn-hangzhou.ots.aliyuncs.com">>,
        storage_model_type => timeseries,
        instance_name => <<"myinstance">>,
        access_key_id => <<"******">>,
        access_key_secret => <<"******">>
    }.

action_parameters_example(timeseries) ->
    #{
        parameters => #{
            storage_model_type => timeseries,
            table_name => <<"table_name">>,
            data_source => <<"${data_source}">>,
            measurement => <<"${measurement}">>,
            tags => #{
                tag1 => <<"${tag1}">>,
                tag2 => <<"${tag2}">>
            },
            fields => [
                #{
                    column => <<"${column}">>,
                    value => <<"${value}">>,
                    isint => true
                }
            ],
            meta_update_model => <<"MUM_IGNORE">>
        }
    }.

values(StorageType, get) ->
    values(StorageType, post);
values(StorageType, put) ->
    values(StorageType, post);
values(timeseries, post) ->
    #{
        name => <<"tablestore_connector">>,
        enable => true,
        local_topic => <<"local/topic/#">>,
        endpoint => <<"https://myinstance.cn-hangzhou.ots.aliyuncs.com">>,
        storage_model_type => timeseries,
        instance_name => <<"myinstance">>,
        access_key_id => <<"******">>,
        access_key_secret => <<"******">>,
        table_name => <<"table_name">>,
        measurement => <<"measurement">>,
        tags => #{
            tag1 => <<"${tag1}">>,
            tag2 => <<"${tag2}">>
        },
        fields => [
            #{
                column => <<"${column}">>,
                value => <<"${value}">>,
                isint => true
            }
        ],
        data_source => <<"${data_source}">>,
        meta_update_model => <<"MUM_IGNORE">>,
        resource_opts => #{
            batch_size => 1,
            batch_time => <<"20ms">>
        },
        pool_size => 8,
        ssl => #{enable => false}
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_tablestore".

roots() -> [].

fields("connector") ->
    [
        storage_model_type_field(),
        {endpoint, mk(binary(), #{required => true, desc => ?DESC("desc_endpoint")})},
        {instance_name, mk(binary(), #{required => true, desc => ?DESC("desc_instance_name")})},
        {access_key_id,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    sensitive => true,
                    desc => ?DESC("desc_access_key_id")
                }
            )},
        {access_key_secret,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    sensitive => true,
                    desc => ?DESC("desc_access_key_secret")
                }
            )},
        {pool_size,
            mk(
                integer(),
                #{
                    required => false,
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(action) ->
    {tablestore,
        mk(
            hoconsc:map(name, ref(?MODULE, tablestore_action)),
            #{desc => <<"Tablestore Action Config">>, required => false}
        )};
fields(tablestore_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        })
    );
fields(action_parameters) ->
    [
        storage_model_type_field(),
        {table_name,
            mk(binary(), #{
                required => true,
                is_template => true,
                desc => ?DESC("desc_table_name")
            })},
        {measurement,
            mk(binary(), #{
                required => true,
                is_template => true,
                desc => ?DESC("desc_measurement")
            })},
        tags_field(),
        fields_field(),
        {data_source,
            mk(binary(), #{
                required => false,
                is_template => true,
                desc => ?DESC("desc_data_source")
            })},
        {timestamp,
            mk(hoconsc:union([integer(), binary()]), #{
                required => false,
                is_template => true,
                desc => ?DESC("desc_timestamp")
            })},
        {meta_update_model,
            mk(
                enum(['MUM_IGNORE', 'MUM_NORMAL']), #{
                    required => false,
                    default => 'MUM_NORMAL',
                    desc => ?DESC("desc_meta_update_model")
                }
            )}
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields("connector") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(tablestore_action));
fields("tablestore_fields") ->
    [
        {column,
            mk(binary(), #{
                required => true,
                is_template => true,
                desc => ?DESC("tablestore_fields_column")
            })},
        {value,
            mk(hoconsc:union([boolean(), number(), binary()]), #{
                required => true,
                is_template => true,
                desc => ?DESC("tablestore_fields_value")
            })},
        {isint,
            mk(hoconsc:union([boolean(), binary()]), #{
                required => false,
                is_template => true,
                desc => ?DESC("tablestore_fields_isint")
            })},
        {isbinary,
            mk(hoconsc:union([boolean(), binary()]), #{
                required => false,
                is_template => true,
                desc => ?DESC("tablestore_fields_isbinary")
            })}
    ].

storage_model_type_field() ->
    {storage_model_type,
        mk(
            enum([timeseries]), #{
                required => false,
                default => timeseries,
                desc => ?DESC("storage_model_type")
            }
        )}.

tags_field() ->
    {tags,
        mk(
            map(), #{
                default => #{},
                desc => ?DESC("desc_tags"),
                is_template => true
            }
        )}.

fields_field() ->
    {fields,
        mk(
            hoconsc:array(ref(?MODULE, "tablestore_fields")), #{
                required => true,
                validator => fun non_empty_list/1,
                desc => ?DESC("desc_fields")
            }
        )}.

non_empty_list([]) -> {error, empty_fields_not_allowed};
non_empty_list(S) when is_list(S) -> ok.

desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Tablestore using `", string:to_upper(Method), "` method."];
desc("connector") ->
    ?DESC("connector");
desc("config_connector") ->
    ?DESC("desc_config");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(tablestore_action) ->
    ?DESC("tablestore_action");
desc("tablestore_fields") ->
    ?DESC("tablestore_fields");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.
