%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_mysql).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_ee_bridge.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived) "
    "values (${id}, ${topic}, ${qos}, ${payload}, FROM_UNIXTIME(${timestamp}/1000))"
>>).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"mysql">> => #{
                summary => <<"MySQL Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(values(post), ?METRICS_EXAMPLE);
values(post) ->
    #{
        type => mysql,
        name => <<"mysql">>,
        connector => #{
            server => <<"127.0.0.1:3306">>,
            database => <<"test">>,
            pool_size => 8,
            username => <<"root">>,
            password => <<"public">>,
            auto_reconnect => true
        },
        enable => true,
        direction => egress,
        sql => ?DEFAULT_SQL
    };
values(put) ->
    values(post).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})},
        {sql, mk(binary(), #{default => ?DEFAULT_SQL, desc => ?DESC("sql")})},
        {connector, field(connector)}
    ];
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post").

field(connector) ->
    mk(
        ref(emqx_ee_connector_mysql, config),
        #{
            required => true,
            desc => ?DESC("desc_connector")
        }
    ).

desc("config") ->
    ?DESC("desc_config");
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
