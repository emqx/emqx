%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_sqlserver).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    bridge_v2_examples/1,
    connector_examples/1,
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, sqlserver).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload) "
    "values ( ${id}, ${topic}, ${qos}, ${payload} )"
>>).

-define(DEFAULT_DRIVER, <<"ms-sql">>).

%% -------------------------------------------------------------------------------------------------
%% api.

conn_bridge_examples(Method) ->
    [
        #{
            <<"sqlserver">> => #{
                summary => <<"Microsoft SQL Server Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    values(post);
values(post) ->
    #{
        enable => true,
        type => sqlserver,
        name => <<"bar">>,
        server => <<"127.0.0.1:1433">>,
        database => <<"test">>,
        pool_size => 8,
        username => <<"sa">>,
        password => <<"******">>,
        sql => ?DEFAULT_SQL,
        driver => ?DEFAULT_DRIVER,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    };
values(put) ->
    values(post).

%% ====================
%% Bridge V2: Connector + Action

connector_examples(Method) ->
    [
        #{
            <<"sqlserver">> =>
                #{
                    summary => <<"Microsoft SQL Server Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        server => <<"127.0.0.1:1433">>,
        database => <<"test">>,
        pool_size => 8,
        username => <<"sa">>,
        password => <<"******">>,
        driver => ?DEFAULT_DRIVER,
        resource_opts => #{health_check_interval => <<"20s">>}
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"sqlserver">> =>
                #{
                    summary => <<"Microsoft SQL Server Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        <<"parameters">> =>
            #{<<"sql">> => ?DEFAULT_SQL}
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_sqlserver".

roots() -> [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(sqlserver_action));
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
fields("config_connector") ->
    driver_fields() ++
        emqx_connector_schema:common_fields() ++
        emqx_bridge_sqlserver_connector:fields(config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )},
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ] ++ driver_fields() ++
        (emqx_bridge_sqlserver_connector:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields(action) ->
    {?ACTION_TYPE,
        mk(
            hoconsc:map(name, ref(?MODULE, sqlserver_action)),
            #{desc => ?DESC("sqlserver_action"), required => false}
        )};
fields(sqlserver_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{required => true, desc => ?DESC(action_parameters)}
        )
    );
fields(action_parameters) ->
    [
        {sql,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ];
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    fields("post", sqlserver);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

driver_fields() ->
    [{driver, mk(binary(), #{desc => ?DESC("driver"), default => ?DEFAULT_DRIVER})}].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Microsoft SQL Server using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc("config_connector") ->
    ?DESC("config_connector");
desc(sqlserver_action) ->
    ?DESC("sqlserver_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
