%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_oracle).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    config_validator/1
]).

-define(CONNECTOR_TYPE, oracle).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msgs(msgid, topic, qos, payload) "
    "values (${id}, ${topic}, ${qos}, ${payload})"
>>).

conn_bridge_examples(_Method) ->
    [
        #{
            <<"oracle">> => #{
                summary => <<"Oracle Database Bridge">>,
                value => conn_bridge_examples_values()
            }
        }
    ].

conn_bridge_examples_values() ->
    #{
        enable => true,
        type => oracle,
        name => <<"foo">>,
        server => <<"127.0.0.1:1521">>,
        pool_size => 8,
        service_name => <<"ORCL">>,
        sid => <<"ORCL">>,
        username => <<"root">>,
        password => <<"******">>,
        sql => ?DEFAULT_SQL,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

connector_examples(Method) ->
    [
        #{
            <<"oracle">> =>
                #{
                    summary => <<"Oracle Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        <<"username">> => <<"system">>,
        <<"password">> => <<"oracle">>,
        <<"server">> => <<"127.0.0.1:1521">>,
        <<"service_name">> => <<"XE">>,
        <<"sid">> => <<"XE">>,
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
            <<"oracle">> =>
                #{
                    summary => <<"Oracle Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        parameters => #{
            <<"sql">> => ?DEFAULT_SQL
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

namespace() -> "bridge_oracle".

roots() -> [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields(connector_fields) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(oracle_action));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, oracle_action)),
            #{
                desc => <<"Oracle Action Config">>,
                required => false
            }
        )};
fields(oracle_action) ->
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
    [
        {sql,
            hoconsc:mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )}
    ];
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields(connector_fields) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    [
        {enable,
            hoconsc:mk(
                boolean(),
                #{desc => ?DESC("config_enable"), default => true}
            )},
        {sql,
            hoconsc:mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {local_topic,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        fields(connector_fields);
fields(connector_fields) ->
    (emqx_oracle_schema:fields(config) --
        emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    fields("post", oracle);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

desc("config") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("config_connector") ->
    ?DESC("config_connector");
desc(oracle_action) ->
    ?DESC("oracle_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, hoconsc:mk(hoconsc:enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, hoconsc:mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

config_validator(#{server := _} = Config) ->
    config_validator(emqx_utils_maps:binary_key_map(Config));
config_validator(#{<<"server">> := Server} = Config) when
    not is_map(Server) andalso
        not is_map_key(<<"sid">>, Config) andalso
        not is_map_key(<<"service_name">>, Config)
->
    {error, "neither SID nor Service Name was set"};
config_validator(_Config) ->
    ok.
