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
        ]).

load_bridges() ->
    ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?MODULE]) ++ ".conf",
    {ok, #{<<"emqx_data_bridge">> := RawConfig}} = hocon:load(ConfFile),
    Bridges = maps:get(<<"bridges">>, RawConfig, []),
    emqx_data_bridge_monitor:ensure_all_started(Bridges).

resource_type(<<"mysql">>) -> emqx_connector_mysql;
resource_type(<<"pgsql">>) -> emqx_connector_pgsql;
resource_type(<<"mongo">>) -> emqx_connector_mongo;
resource_type(<<"redis">>) -> emqx_connector_redis;
resource_type(<<"ldap">>) -> emqx_connector_ldap.


bridge_type(emqx_connector_mysql) -> <<"mysql">>;
bridge_type(emqx_connector_pgsql) -> <<"pgsql">>;
bridge_type(emqx_connector_mongo) -> <<"mongo">>;
bridge_type(emqx_connector_redis) -> <<"redis">>;
bridge_type(emqx_connector_ldap) -> <<"ldap">>.


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
