-module(emqx_data_bridge).

-export([ load_bridges/0
        ]).

load_bridges() ->
    Bridges = proplists:get_value(bridges,
        application:get_all_env(emqx_data_bridge), []),
    emqx_data_bridge_monitor:ensure_all_started(Bridges).
