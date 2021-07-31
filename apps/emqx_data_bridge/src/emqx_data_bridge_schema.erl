-module(emqx_data_bridge_schema).

-export([structs/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

-define(TYPES, [mysql, pgsql, mongo, redis, ldap]).
-define(BRIDGES, [hoconsc:ref(?MODULE, T) || T <- ?TYPES]).

structs() -> ["emqx_data_bridge"].

fields("emqx_data_bridge") ->
    [{bridges, #{type => hoconsc:array(hoconsc:union(?BRIDGES)),
                 default => []}}];

fields(mysql) -> connector_fields(mysql);
fields(pgsql) -> connector_fields(pgsql);
fields(mongo) -> connector_fields(mongo);
fields(redis) -> connector_fields(redis);
fields(ldap)  -> connector_fields(ldap).

connector_fields(DB) ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
    [{name, hoconsc:t(typerefl:binary())},
     {type, #{type => DB}}] ++ Mod:fields("").
