%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_data_bridge).

-export([ load_bridges/0
        , resource_type/1
        , bridge_type/1
        , name_to_resource_id/1
        , resource_id_to_name/1
        , list_bridges/0
        , is_bridge/1
        , config_key_path/0
        , update_config/1
        ]).

-export([structs/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

-define(BRIDGE_FIELDS(T),
    [{name, hoconsc:t(typerefl:binary())},
     {type, hoconsc:t(typerefl:atom(T))},
     {config, hoconsc:t(hoconsc:ref(list_to_atom("emqx_connector_"++atom_to_list(T)), ""))}]).

-define(TYPES, [mysql, pgsql, mongo, redis, ldap]).
-define(BRIDGES, [hoconsc:ref(?MODULE, T) || T <- ?TYPES]).

structs() -> ["emqx_data_bridge"].

fields("emqx_data_bridge") ->
    [{bridges, #{type => hoconsc:array(hoconsc:union(?BRIDGES)),
                 default => []}}];

fields(mysql) -> ?BRIDGE_FIELDS(mysql);
fields(pgsql) -> ?BRIDGE_FIELDS(pgsql);
fields(mongo) -> ?BRIDGE_FIELDS(mongo);
fields(redis) -> ?BRIDGE_FIELDS(redis);
fields(ldap) -> ?BRIDGE_FIELDS(ldap).

%%======================================================================================

load_bridges() ->
    % ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?MODULE]) ++ ".conf",
    % {ok, RawConfig} = hocon:load(ConfFile, #{format => richmap}),
    % #{emqx_data_bridge := #{bridges := Bridges}} =
    %     hocon_schema:check(emqx_data_bridge_schema, RawConfig,
    %         #{atom_key => true, return_plain => true}),
    Bridges = emqx_config:get([emqx_data_bridge, bridges], []),
    emqx_data_bridge_monitor:ensure_all_started(Bridges).

resource_type(mysql) -> emqx_connector_mysql;
resource_type(pgsql) -> emqx_connector_pgsql;
resource_type(mongo) -> emqx_connector_mongo;
resource_type(redis) -> emqx_connector_redis;
resource_type(ldap) -> emqx_connector_ldap.

bridge_type(emqx_connector_mysql) -> mysql;
bridge_type(emqx_connector_pgsql) -> pgsql;
bridge_type(emqx_connector_mongo) -> mongo;
bridge_type(emqx_connector_redis) -> redis;
bridge_type(emqx_connector_ldap) -> ldap.

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

config_key_path() ->
    [emqx_data_bridge, bridges].

update_config(ConfigReq) ->
    emqx_config_handler:update_config(config_key_path(), ConfigReq).
