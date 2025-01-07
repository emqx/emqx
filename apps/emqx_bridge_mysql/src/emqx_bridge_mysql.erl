%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, mysql).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived) "
    "values (${id}, ${topic}, ${qos}, ${payload}, FROM_UNIXTIME(${timestamp}/1000))"
>>).

%% -------------------------------------------------------------------------------------------------
%% api

bridge_v2_examples(Method) ->
    [
        #{
            <<"mysql">> =>
                #{
                    summary => <<"MySQL Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{parameters => #{sql => ?DEFAULT_SQL}}.

conn_bridge_examples(Method) ->
    [
        #{
            <<"mysql">> => #{
                summary => <<"MySQL Bridge">>,
                value => values(Method)
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"mysql">> =>
                #{
                    summary => <<"MySQL Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        server => <<"127.0.0.1:3306">>,
        database => <<"test">>,
        pool_size => 8,
        username => <<"root">>,
        password => <<"******">>,
        resource_opts => #{health_check_interval => <<"20s">>}
    }.

values(_Method) ->
    #{
        enable => true,
        type => mysql,
        name => <<"foo">>,
        server => <<"127.0.0.1:3306">>,
        database => <<"test">>,
        pool_size => 8,
        username => <<"root">>,
        password => <<"******">>,
        sql => ?DEFAULT_SQL,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_mysql".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        emqx_mysql:fields(config);
fields(action) ->
    {mysql,
        mk(
            hoconsc:map(name, ref(?MODULE, mysql_action)),
            #{desc => ?DESC("mysql_action"), required => false}
        )};
fields(mysql_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{
                required => true, desc => ?DESC(action_parameters)
            }
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
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_mysql:fields(config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(mysql_action));
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        emqx_mysql:fields(config) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts)
    ).

desc("config") ->
    ?DESC("desc_config");
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(mysql_action) ->
    ?DESC(mysql_action);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for MySQL using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal

type_field() ->
    {type, mk(enum([mysql]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
