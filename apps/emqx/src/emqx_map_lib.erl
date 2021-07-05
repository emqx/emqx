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
-module(emqx_map_lib).

-export([ deep_get/2
        , deep_get/3
        , deep_find/2
        , deep_put/3
        , safe_atom_key_map/1
        , unsafe_atom_key_map/1
        ]).

-export_type([config_key/0, config_key_path/0]).
-type config_key() :: atom() | binary().
-type config_key_path() :: [config_key()].

%%-----------------------------------------------------------------
-spec deep_get(config_key_path(), map()) -> term().
deep_get(ConfKeyPath, Map) ->
    case deep_find(ConfKeyPath, Map) of
        {not_found, KeyPath, Data} -> error({not_found, KeyPath, Data});
        {ok, Data} -> Data
    end.

-spec deep_get(config_key_path(), map(), term()) -> term().
deep_get(ConfKeyPath, Map, Default) ->
    case deep_find(ConfKeyPath, Map) of
        {not_found, _KeyPath, _Data} -> Default;
        {ok, Data} -> Data
    end.

-spec deep_find(config_key_path(), map()) -> {ok, term()} | {not_found, config_key(), term()}.
deep_find([], Map) ->
    {ok, Map};
deep_find([Key | KeyPath], Map) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} -> deep_find(KeyPath, SubMap);
        error -> {not_found, Key, Map}
    end;
deep_find([Key | _KeyPath], Data) ->
    {not_found, Key, Data}.

-spec deep_put(config_key_path(), map(), term()) -> map().
deep_put([], Map, Config) when is_map(Map) ->
    Config;
deep_put([Key | KeyPath], Map, Config) ->
    SubMap = deep_put(KeyPath, maps:get(Key, Map, #{}), Config),
    Map#{Key => SubMap}.

unsafe_atom_key_map(Map) ->
    covert_keys_to_atom(Map, fun(K) -> binary_to_atom(K, utf8) end).

safe_atom_key_map(Map) ->
    covert_keys_to_atom(Map, fun(K) -> binary_to_existing_atom(K, utf8) end).

%%---------------------------------------------------------------------------
covert_keys_to_atom(BinKeyMap, Conv) when is_map(BinKeyMap) ->
    maps:fold(
        fun(K, V, Acc) when is_binary(K) ->
              Acc#{Conv(K) => covert_keys_to_atom(V, Conv)};
           (K, V, Acc) when is_atom(K) ->
              %% richmap keys
              Acc#{K => covert_keys_to_atom(V, Conv)}
        end, #{}, BinKeyMap);
covert_keys_to_atom(ListV, Conv) when is_list(ListV) ->
    [covert_keys_to_atom(V, Conv) || V <- ListV];
covert_keys_to_atom(Val, _) -> Val.
