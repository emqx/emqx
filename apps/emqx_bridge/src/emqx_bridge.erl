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
-module(emqx_bridge).
-behaviour(emqx_config_handler).

-export([post_config_update/4]).

-export([ load_bridges/0
        , get_bridge/2
        , get_bridge/3
        , list_bridges/0
        , create_bridge/3
        , remove_bridge/3
        , update_bridge/3
        , start_bridge/2
        , stop_bridge/2
        , restart_bridge/2
        ]).

-export([ config_key_path/0
        ]).

-export([ resource_type/1
        , bridge_type/1
        , resource_id/1
        , resource_id/2
        , parse_bridge_id/1
        ]).

config_key_path() ->
    [bridges].

resource_type(mqtt) -> emqx_connector_mqtt;
resource_type(mysql) -> emqx_connector_mysql;
resource_type(pgsql) -> emqx_connector_pgsql;
resource_type(mongo) -> emqx_connector_mongo;
resource_type(redis) -> emqx_connector_redis;
resource_type(ldap) -> emqx_connector_ldap.

bridge_type(emqx_connector_mqtt) -> mqtt;
bridge_type(emqx_connector_mysql) -> mysql;
bridge_type(emqx_connector_pgsql) -> pgsql;
bridge_type(emqx_connector_mongo) -> mongo;
bridge_type(emqx_connector_redis) -> redis;
bridge_type(emqx_connector_ldap) -> ldap.

post_config_update(_Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated}
        = diff_confs(NewConf, OldConf),
    perform_bridge_changes([
        {fun remove_bridge/3, Removed},
        {fun create_bridge/3, Added},
        {fun update_bridge/3, Updated}
    ]).

perform_bridge_changes(Tasks) ->
    perform_bridge_changes(Tasks, ok).

perform_bridge_changes([], Result) ->
    Result;
perform_bridge_changes([{Action, MapConfs} | Tasks], Result0) ->
    Result = maps:fold(fun
        ({_Type, _Name}, _Conf, {error, Reason}) ->
            {error, Reason};
        ({Type, Name}, Conf, _) ->
            case Action(Type, Name, Conf) of
                {error, Reason} -> {error, Reason};
                Return -> Return
            end
        end, Result0, MapConfs),
    perform_bridge_changes(Tasks, Result).

load_bridges() ->
    Bridges = emqx:get_config([bridges], #{}),
    emqx_bridge_monitor:ensure_all_started(Bridges).

resource_id(BridgeId) when is_binary(BridgeId) ->
    <<"bridge:", BridgeId/binary>>.

resource_id(BridgeType, BridgeName) ->
    BridgeId = bridge_id(BridgeType, BridgeName),
    resource_id(BridgeId).

bridge_id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

parse_bridge_id(BridgeId) ->
    try
        [Type, Name] = string:split(str(BridgeId), ":", leading),
        {list_to_existing_atom(Type), list_to_atom(Name)}
    catch
        _ : _ -> error({invalid_bridge_id, BridgeId})
    end.

list_bridges() ->
    lists:foldl(fun({Type, NameAndConf}, Bridges) ->
            lists:foldl(fun({Name, RawConf}, Acc) ->
                    case get_bridge(Type, Name, RawConf) of
                        {error, not_found} -> Acc;
                        {ok, Res} -> [Res | Acc]
                    end
                end, Bridges, maps:to_list(NameAndConf))
        end, [], maps:to_list(emqx:get_raw_config([bridges]))).

get_bridge(Type, Name) ->
    RawConf = emqx:get_raw_config([bridges, Type, Name], #{}),
    get_bridge(Type, Name, RawConf).
get_bridge(Type, Name, RawConf) ->
    case emqx_resource:get_instance(resource_id(Type, Name)) of
        {error, not_found} -> {error, not_found};
        {ok, Data} -> {ok, #{id => bridge_id(Type, Name), resource_data => Data,
                             raw_config => RawConf}}
    end.

start_bridge(Type, Name) ->
    restart_bridge(Type, Name).

stop_bridge(Type, Name) ->
    emqx_resource:stop(resource_id(Type, Name)).

restart_bridge(Type, Name) ->
    emqx_resource:restart(resource_id(Type, Name)).

create_bridge(Type, Name, Conf) ->
    logger:info("create ~p bridge ~p use config: ~p", [Type, Name, Conf]),
    ResId = resource_id(Type, Name),
    case emqx_resource:create(ResId,
            emqx_bridge:resource_type(Type), Conf) of
        {ok, already_created} ->
            emqx_resource:get_instance(ResId);
        {ok, Data} ->
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

update_bridge(Type, Name, {_OldConf, Conf}) ->
    %% TODO: sometimes its not necessary to restart the bridge connection.
    %%
    %% - if the connection related configs like `username` is updated, we should restart/start
    %% or stop bridges according to the change.
    %% - if the connection related configs are not update, but channel configs `ingress_channels` or
    %% `egress_channels` are changed, then we should not restart the bridge, we only restart/start
    %% the channels.
    %%
    logger:info("update ~p bridge ~p use config: ~p", [Type, Name, Conf]),
    emqx_resource:recreate(resource_id(Type, Name),
        emqx_bridge:resource_type(Type), Conf, []).

remove_bridge(Type, Name, _Conf) ->
    logger:info("remove ~p bridge ~p", [Type, Name]),
    case emqx_resource:remove(resource_id(Type, Name)) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} ->
            {error, Reason}
    end.

diff_confs(NewConfs, OldConfs) ->
    emqx_map_lib:diff_maps(flatten_confs(NewConfs),
        flatten_confs(OldConfs)).

flatten_confs(Conf0) ->
    maps:from_list(
        lists:flatmap(fun({Type, Conf}) ->
                do_flatten_confs(Type, Conf)
            end, maps:to_list(Conf0))).

do_flatten_confs(Type, Conf0) ->
    [{{Type, Name}, Conf} || {Name, Conf} <- maps:to_list(Conf0)].

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
