%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

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

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"greptimedb">> => #{
                summary => <<"Greptimedb HTTP API V2 Bridge">>,
                value => values("greptimedb", Method)
            }
        }
    ].

values(Protocol, get) ->
    values(Protocol, post);
values("greptimedb", post) ->
    SupportUint = <<"uint_value=${payload.uint_key}u,">>,
    TypeOpts = #{
        bucket => <<"example_bucket">>,
        org => <<"examlpe_org">>,
        token => <<"example_token">>,
        server => <<"127.0.0.1:4001">>
    },
    values(common, "greptimedb", SupportUint, TypeOpts);
values(Protocol, put) ->
    values(Protocol, post).

values(common, Protocol, SupportUint, TypeOpts) ->
    CommonConfigs = #{
        type => list_to_atom(Protocol),
        name => <<"demo">>,
        enable => true,
        local_topic => <<"local/topic/#">>,
        write_syntax =>
            <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
                "${clientid}_int_value=${payload.int_key}i,", SupportUint/binary,
                "bool=${payload.bool}">>,
        precision => ms,
        resource_opts => #{
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        server => <<"127.0.0.1:4001">>,
        ssl => #{enable => false}
    },
    maps:merge(TypeOpts, CommonConfigs).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_greptimedb".

roots() -> [].

fields("post_grpc_v1") ->
    method_fields(post, greptimedb);
fields("put_grpc_v1") ->
    method_fields(put, greptimedb);
fields("get_grpc_v1") ->
    method_fields(get, greptimedb);
fields(Type) when
    Type == greptimedb
->
    greptimedb_bridge_common_fields() ++
        connector_fields(Type).

method_fields(post, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType);
method_fields(get, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType) ++
        emqx_bridge_schema:status_fields();
method_fields(put, ConnectorType) ->
    greptimedb_bridge_common_fields() ++
        connector_fields(ConnectorType).

greptimedb_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {write_syntax, fun write_syntax/1}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields(Type) ->
    emqx_bridge_greptimedb_connector:fields(Type).

type_name_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Greptimedb using `", string:to_upper(Method), "` method."];
desc(greptimedb) ->
    ?DESC(emqx_bridge_greptimedb_connector, "greptimedb");
desc(_) ->
    undefined.

write_syntax(type) ->
    emqx_bridge_influxdb:write_syntax();
write_syntax(required) ->
    true;
write_syntax(validator) ->
    [?NOT_EMPTY("the value of the field 'write_syntax' cannot be empty")];
write_syntax(converter) ->
    fun emqx_bridge_influxdb:to_influx_lines/1;
write_syntax(desc) ->
    ?DESC("write_syntax");
write_syntax(format) ->
    <<"sql">>;
write_syntax(_) ->
    undefined.
