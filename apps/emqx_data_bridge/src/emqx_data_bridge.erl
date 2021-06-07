-module(emqx_data_bridge).

-export([ load_bridges/0
        , resource_type/1
        , bridge_type/1
        , name_to_resource_id/1
        , resource_id_to_name/1
        , list_bridges/0
        , is_bridge/1
        ]).

load_bridges() ->
    Bridges = proplists:get_value(bridges,
        application:get_all_env(emqx_data_bridge), []),
    emqx_data_bridge_monitor:ensure_all_started(Bridges).

resource_type(<<"mysql">>) -> emqx_connector_mysql.

bridge_type(emqx_connector_mysql) -> <<"mysql">>.

name_to_resource_id(BridgeName) ->
    <<"bridge:", BridgeName/binary>>.

resource_id_to_name(<<"bridge:", BridgeName/binary>> = _ResourceId) ->
    BridgeName.

list_bridges() ->
    emqx_resource_api:list_instances(fun emqx_data_bridge:is_bridge/1).

is_bridge(#{id := <<"bridge:", _/binary>>}) ->
    true;
is_bridge(_Data) ->
    false.
