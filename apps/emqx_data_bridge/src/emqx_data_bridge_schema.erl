-module(emqx_data_bridge_schema).

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

-define(TYPES, [mysql, pgsql, mongo, redis, ldap]).
-define(BRIDGES, [hoconsc:ref(?MODULE, T) || T <- ?TYPES]).

roots() -> ["emqx_data_bridge"].

fields("emqx_data_bridge") ->
    [{bridges, #{type => hoconsc:array(hoconsc:union(?BRIDGES)),
                 default => []}}];

fields(mysql) -> connector_fields(emqx_connector_mysql, mysql);
fields(pgsql) -> connector_fields(emqx_connector_pgsql, pgsql);
fields(mongo) -> connector_fields(emqx_connector_mongo, mongo);
fields(redis) -> connector_fields(emqx_connector_redis, redis);
fields(ldap)  -> connector_fields(emqx_connector_ldap, ldap).

connector_fields(ConnectModule, DB) ->
    [{name, hoconsc:mk(typerefl:binary())},
     {type, #{type => DB}}] ++ ConnectModule:roots().
