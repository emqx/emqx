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
    emqx_bridge_schema:common_bridge_fields() ++
        emqx_connector_mqtt_schema:fields("config");
fields("post") ->
    [
        type_field(),
        name_field()
    ] ++ emqx_connector_mqtt_schema:fields("config");
fields("put") ->
    emqx_connector_mqtt_schema:fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("config").

desc(_) ->
    undefined.

%%======================================================================================
type_field() ->
    {type,
        mk(
            mqtt,
            #{
                required => true,
                desc => ?DESC("desc_type")
            }
        )}.

name_field() ->
    {name,
        mk(
            binary(),
            #{
                required => true,
                desc => ?DESC("desc_name")
            }
        )}.
