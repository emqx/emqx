%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dynamo).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    values/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    bridge_v2_examples/1,
    connector_examples/1,
    conn_bridge_examples/1
]).

-define(CONNECTOR_TYPE, dynamo).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).
-define(DEFAULT_TEMPLATE, <<>>).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"dynamo">> => #{
                summary => <<"DynamoDB Bridge">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enable => true,
        type => dynamo,
        name => <<"foo">>,
        url => <<"http://127.0.0.1:8000">>,
        table => <<"mqtt">>,
        pool_size => 8,
        aws_access_key_id => <<"root">>,
        aws_secret_access_key => <<"******">>,
        template => ?DEFAULT_TEMPLATE,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

connector_examples(Method) ->
    [
        #{
            <<"dynamo">> =>
                #{
                    summary => <<"DynamoDB Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        <<"enable">> => true,
        <<"url">> => <<"http://127.0.0.1:8000">>,
        <<"aws_access_key_id">> => <<"root">>,
        <<"aws_secret_access_key">> => <<"******">>,
        <<"region">> => <<"us-west-2">>,
        <<"pool_size">> => 8,
        <<"resource_opts">> =>
            #{
                <<"health_check_interval">> => <<"15s">>,
                <<"start_timeout">> => <<"5s">>
            }
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"dynamo">> =>
                #{
                    summary => <<"DynamoDB Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        <<"parameters">> =>
            #{
                <<"table">> => <<"mqtt_msg">>,
                <<"template">> => ?DEFAULT_TEMPLATE,
                <<"hash_key">> => <<"clientid">>
            }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_dynamo".

roots() -> [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        fields("config_connector") -- emqx_connector_schema:common_fields()
    );
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(dynamo_action));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, dynamo_action)),
            #{
                desc => <<"DynamoDB Action Config">>,
                required => false
            }
        )};
fields(dynamo_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields(action_parameters) ->
    Parameters =
        [
            {template, template_field_schema()},
            {hash_key,
                mk(
                    binary(),
                    #{desc => ?DESC("hash_key"), required => true}
                )},
            {range_key,
                mk(
                    binary(),
                    #{desc => ?DESC("range_key"), required => false}
                )}
        ] ++ emqx_bridge_dynamo_connector:fields(config),
    lists:foldl(
        fun(Key, Acc) ->
            proplists:delete(Key, Acc)
        end,
        Parameters,
        [
            url,
            region,
            aws_access_key_id,
            aws_secret_access_key,
            pool_size,
            auto_reconnect
        ]
    );
fields("config_connector") ->
    Config =
        emqx_connector_schema:common_fields() ++
            emqx_bridge_dynamo_connector:fields(config) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    lists:foldl(
        fun(Key, Acc) ->
            proplists:delete(Key, Acc)
        end,
        Config,
        [
            table,
            undefined_vars_as_null
        ]
    );
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {template, template_field_schema()},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )},
        {hash_key,
            mk(
                binary(),
                #{desc => ?DESC("hash_key"), required => true}
            )},
        {range_key,
            mk(
                binary(),
                #{desc => ?DESC("range_key"), required => false}
            )},
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ] ++
        (emqx_bridge_dynamo_connector:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

template_field_schema() ->
    mk(
        emqx_schema:template(),
        #{
            desc => ?DESC("template"),
            default => ?DEFAULT_TEMPLATE
        }
    ).

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for DynamoDB using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc("config_connector") ->
    ?DESC("config_connector");
desc(dynamo_action) ->
    ?DESC("dynamo_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([dynamo]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
