%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

-export([type_name_fields/1, connector_fields/1]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_redis".

roots() -> [].

fields(action_parameters) ->
    [
        command_template(),
        {redis_type,
            ?HOCON(
                ?ENUM([single, sentinel, cluster]), #{
                    required => false,
                    desc => ?DESC(redis_type),
                    hidden => true,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ].

connector_fields(Type) ->
    emqx_redis:fields(Type).

type_name_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc(action_parameters) ->
    ?DESC("desc_action_parameters");
desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Redis using `", string:to_upper(Method), "` method."];
desc(redis_single) ->
    ?DESC(emqx_redis, "single");
desc(redis_sentinel) ->
    ?DESC(emqx_redis, "sentinel");
desc(redis_cluster) ->
    ?DESC(emqx_redis, "cluster");
desc("creation_opts_" ++ _Type) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_) ->
    undefined.

command_template(type) ->
    hoconsc:array(emqx_schema:template());
command_template(required) ->
    true;
command_template(validator) ->
    fun is_command_template_valid/1;
command_template(desc) ->
    ?DESC("command_template");
command_template(_) ->
    undefined.

is_command_template_valid(CommandSegments) ->
    case
        is_list(CommandSegments) andalso length(CommandSegments) > 0 andalso
            lists:all(fun is_binary/1, CommandSegments)
    of
        true ->
            ok;
        false ->
            {error,
                "the value of the field 'command_template' should be a nonempty "
                "list of strings (templates for Redis command and arguments)"}
    end.

command_template() ->
    {command_template, fun command_template/1}.
