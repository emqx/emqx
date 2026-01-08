%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq).

-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
%% -------------------------------------------------------------------------------------------------

namespace() -> "bridge_rabbitmq".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
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
        emqx_bridge_rabbitmq_connector:fields(config);
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    fields("post", rabbitmq);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_v2_api:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for RabbitMQ using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal
%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
