%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_hstreamdb).

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
            <<"hstreamdb">> => #{
                summary => <<"HStreamDB Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    values(post);
values(put) ->
    values(post);
values(post) ->
    #{
        type => <<"hstreamdb">>,
        name => <<"demo">>,
        direction => <<"egress">>,
        url => <<"http://127.0.0.1:6570">>,
        stream => <<"stream">>,
        %% raw HRecord
        record_template =>
            <<"{ \"temperature\": ${payload.temperature}, \"humidity\": ${payload.humidity} }">>,
        pool_size => 8,
        %% grpc_timeout => <<"1m">>
        resource_opts => #{
            query_mode => sync,
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        ssl => #{enable => false}
    };
values(_) ->
    #{}.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_hstreamdb".

roots() -> [].

fields("config") ->
    hstream_bridge_common_fields() ++
        connector_fields();
fields("post") ->
    hstream_bridge_common_fields() ++
        connector_fields() ++
        type_name_fields();
fields("get") ->
    hstream_bridge_common_fields() ++
        connector_fields() ++
        type_name_fields() ++
        emqx_bridge_schema:status_fields();
fields("put") ->
    hstream_bridge_common_fields() ++
        connector_fields().

hstream_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})},
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {record_template,
                mk(binary(), #{default => <<"${payload}">>, desc => ?DESC("record_template")})}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields() ->
    emqx_bridge_hstreamdb_connector:fields(config).

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for HStreamDB bridge using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal
type_name_fields() ->
    [
        {type, mk(enum([hstreamdb]), #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].
