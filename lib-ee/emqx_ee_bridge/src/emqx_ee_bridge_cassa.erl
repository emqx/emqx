%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_cassa).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

%% schema examples
-export([
    conn_bridge_examples/1,
    values/2,
    fields/2
]).

%% schema
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_CQL, <<
    "insert into mqtt_msg(topic, msgid, sender, qos, payload, arrived, retain) "
    "values (${topic}, ${id}, ${clientid}, ${qos}, ${payload}, ${timestamp}, ${flags.retain})"
>>).

%%--------------------------------------------------------------------
%% schema examples

conn_bridge_examples(Method) ->
    [
        #{
            <<"cassandra">> => #{
                summary => <<"Cassandra Bridge">>,
                value => values(Method, cassandra)
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
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            auto_restart_interval => ?AUTO_RESTART_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_queue_bytes => ?DEFAULT_QUEUE_SIZE
        }
    }.

%%--------------------------------------------------------------------
%% schema

namespace() -> "bridge_cassa".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {cql,
            mk(
                binary(),
                #{desc => ?DESC("cql_template"), default => ?DEFAULT_CQL, format => <<"sql">>}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        (emqx_ee_connector_cassa:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    fields("post", cassandra);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Cassandra using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% utils

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
