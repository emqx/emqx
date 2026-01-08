%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_cassandra).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

%% schema examples
-export([
    values/2,
    fields/2
]).

%% Examples
-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

%% schema
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_CQL, <<
    "insert into mqtt_msg(msgid, topic, qos, payload, arrived) "
    "values (${id}, ${topic},  ${qos}, ${payload}, ${timestamp})"
>>).

-define(CONNECTOR_TYPE, cassandra).
-define(ACTION_TYPE, cassandra).

%%--------------------------------------------------------------------
%% schema examples

bridge_v2_examples(Method) ->
    ParamsExample = #{
        parameters => #{
            cql => ?DEFAULT_CQL
        }
    },
    [
        #{
            <<"cassandra">> => #{
                summary => <<"Cassandra Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, cassandra, cassandra, ParamsExample
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"cassandra">> => #{
                summary => <<"Cassandra Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, cassandra, #{
                        servers => <<"127.0.0.1:9042">>,
                        keyspace => <<"mqtt">>,
                        username => <<"root">>,
                        password => <<"******">>,
                        pool_size => 8
                    }
                )
            }
        }
    ].

%% no difference in get/post/put method
values(_Method, Type) ->
    #{
        enable => true,
        type => Type,
        name => <<"foo">>,
        servers => <<"127.0.0.1:9042">>,
        keyspace => <<"mqtt">>,
        pool_size => 8,
        username => <<"root">>,
        password => <<"******">>,
        cql => ?DEFAULT_CQL,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%%--------------------------------------------------------------------
%% schema

namespace() -> "bridge_cassa".

roots() -> [].

fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_cassandra_connector:fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(action) ->
    {cassandra,
        mk(
            hoconsc:map(name, ref(?MODULE, cassandra_action)),
            #{desc => <<"Cassandra Action Config">>, required => false}
        )};
fields(cassandra_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        }),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(action_parameters) ->
    [
        cql_field()
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 100}},
        {batch_time, #{default => <<"100ms">>}}
    ]);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        emqx_bridge_cassandra_connector:fields("connector") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(cassandra_action));
fields("config") ->
    [
        cql_field(),
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        (emqx_bridge_cassandra_connector:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    fields("post", cassandra);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_v2_api:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

cql_field() ->
    {cql,
        mk(
            emqx_schema:template(),
            #{desc => ?DESC("cql_template"), default => ?DEFAULT_CQL, format => <<"sql">>}
        )}.

desc("config") ->
    ?DESC("desc_config");
desc(cassandra_action) ->
    ?DESC(cassandra_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(action_resource_opts) ->
    emqx_bridge_v2_schema:desc(action_resource_opts);
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Cassandra using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% utils

ref(StructName) -> hoconsc:ref(?MODULE, StructName).

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
