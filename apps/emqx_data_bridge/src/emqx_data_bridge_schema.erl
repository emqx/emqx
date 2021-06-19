-module(emqx_data_bridge_schema).

-export([structs/0, fields/1]).

-define(BRIDGE_FIELDS(T),
    [{name, hoconsc:t(typerefl:binary())},
     {type, hoconsc:t(typerefl:atom(T))},
     {config, hoconsc:t(hoconsc:ref(list_to_atom("emqx_connector_"++atom_to_list(T)), ""))}]).

-define(TYPES, [mysql, pgsql, mongo, redis, ldap]).
-define(BRIDGES, [hoconsc:ref(T) || T <- ?TYPES]).

structs() -> [emqx_data_bridge].

fields(emqx_data_bridge) ->
    [{bridges, #{type => hoconsc:array(hoconsc:union(?BRIDGES)),
                 default => []}}];

fields(mysql) -> ?BRIDGE_FIELDS(mysql);
fields(pgsql) -> ?BRIDGE_FIELDS(pgsql);
fields(mongo) -> ?BRIDGE_FIELDS(mongo);
fields(redis) -> ?BRIDGE_FIELDS(redis);
fields(ldap) -> ?BRIDGE_FIELDS(ldap).
