-module(emqx_bridge_mqtt_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_mqtt".

roots() -> [].

fields("config") ->
    %% enable
    emqx_bridge_schema:common_bridge_fields() ++
        [
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
        emqx_connector_mqtt_schema:fields("config");
fields("creation_opts") ->
    Opts = emqx_resource_schema:fields("creation_opts"),
    [O || {Field, _} = O <- Opts, not is_hidden_opts(Field)];
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("config").

desc("config") ->
    ?DESC("config");
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%%======================================================================================
%% internal
is_hidden_opts(Field) ->
    lists:member(Field, [enable_batch, batch_size, batch_time]).

type_field() ->
    {type, mk(mqtt, #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
