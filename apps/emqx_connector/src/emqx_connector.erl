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
-module(emqx_connector).

-export([config_key_path/0]).

-export([ parse_connector_id/1
        , connector_id/2
        ]).

-export([ list/0
        , lookup/1
        , lookup/2
        , create_dry_run/2
        , update/2
        , update/3
        , delete/1
        , delete/2
        ]).

-export([ post_config_update/5
        ]).

config_key_path() ->
    [connectors].

post_config_update([connectors, Type, Name], '$remove', _, _OldConf, _AppEnvs) ->
    ConnId = connector_id(Type, Name),
    LinkedBridgeIds = lists:foldl(fun
        (#{id := BId, raw_config := #{<<"connector">> := ConnId0}}, Acc)
                when ConnId0 == ConnId ->
            [BId | Acc];
        (_, Acc) -> Acc
    end, [], emqx_bridge:list()),
    case LinkedBridgeIds of
        [] -> ok;
        _ -> {error, {dependency_bridges_exist, LinkedBridgeIds}}
    end;
post_config_update([connectors, Type, Name], _Req, NewConf, _OldConf, _AppEnvs) ->
    ConnId = connector_id(Type, Name),
    lists:foreach(fun
        (#{id := BId, raw_config := #{<<"connector">> := ConnId0}}) when ConnId0 == ConnId ->
            {BType, BName} = emqx_bridge:parse_bridge_id(BId),
            BridgeConf = emqx:get_config([bridges, BType, BName]),
            case emqx_bridge:recreate(BType, BName, BridgeConf#{connector => NewConf}) of
                {ok, _} -> ok;
                {error, Reason} -> error({update_bridge_error, Reason})
            end;
        (_) ->
            ok
    end, emqx_bridge:list()).

connector_id(Type0, Name0) ->
    Type = bin(Type0),
    Name = bin(Name0),
    <<Type/binary, ":", Name/binary>>.

parse_connector_id(ConnectorId) ->
    case string:split(bin(ConnectorId), ":", all) of
        [Type, Name] -> {binary_to_atom(Type, utf8), binary_to_atom(Name, utf8)};
        _ -> error({invalid_connector_id, ConnectorId})
    end.

list() ->
    lists:foldl(fun({Type, NameAndConf}, Connectors) ->
            lists:foldl(fun({Name, RawConf}, Acc) ->
                   [RawConf#{<<"id">> => connector_id(Type, Name)} | Acc]
                end, Connectors, maps:to_list(NameAndConf))
        end, [], maps:to_list(emqx:get_raw_config(config_key_path(), #{}))).

lookup(Id) when is_binary(Id) ->
    {Type, Name} = parse_connector_id(Id),
    lookup(Type, Name).

lookup(Type, Name) ->
    Id = connector_id(Type, Name),
    case emqx:get_raw_config(config_key_path() ++ [Type, Name], not_found) of
        not_found -> {error, not_found};
        Conf -> {ok, Conf#{<<"id">> => Id}}
    end.

create_dry_run(Type, Conf) ->
    emqx_bridge:create_dry_run(Type, Conf).

update(Id, Conf) when is_binary(Id) ->
    {Type, Name} = parse_connector_id(Id),
    update(Type, Name, Conf).

update(Type, Name, Conf) ->
    emqx_conf:update(config_key_path() ++ [Type, Name], Conf, #{override_to => cluster}).

delete(Id) when is_binary(Id) ->
    {Type, Name} = parse_connector_id(Id),
    delete(Type, Name).

delete(Type, Name) ->
    emqx_conf:remove(config_key_path() ++ [Type, Name], #{override_to => cluster}).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
