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
-module(emqx_config).

-compile({no_auto_import, [get/0, get/1]}).

-export([ get/0
        , get/1
        , get/2
        , put/1
        , put/2
        ]).

-export([ update_config/2
        ]).

%% raw configs is the config that is now parsed and tranlated by hocon schema
-export([ get_raw/0
        , get_raw/1
        , get_raw/2
        , put_raw/1
        , put_raw/2
        ]).

-export([ deep_get/2
        , deep_get/3
        , deep_put/3
        , safe_atom_key_map/1
        , unsafe_atom_key_map/1
        ]).

-define(CONF, ?MODULE).
-define(RAW_CONF, {?MODULE, raw}).

-export_type([update_request/0, raw_config/0, config_key/0, config_key_path/0]).
-type update_request() :: term().
-type raw_config() :: hocon:config() | undefined.
-type config_key() :: atom() | binary().
-type config_key_path() :: [config_key()].

-spec get() -> map().
get() ->
    persistent_term:get(?CONF, #{}).

-spec get(config_key_path()) -> term().
get(KeyPath) ->
    deep_get(KeyPath, get()).

-spec get(config_key_path(), term()) -> term().
get(KeyPath, Default) ->
    deep_get(KeyPath, get(), Default).

-spec put(map()) -> ok.
put(Config) ->
    persistent_term:put(?CONF, Config).

-spec put(config_key_path(), term()) -> ok.
put(KeyPath, Config) ->
    put(deep_put(KeyPath, get(), Config)).

-spec update_config(config_key_path(), update_request()) ->
    ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq) ->
    emqx_config_handler:update_config(ConfKeyPath, UpdateReq, get_raw()).

-spec get_raw() -> map().
get_raw() ->
    persistent_term:get(?RAW_CONF, #{}).

-spec get_raw(config_key_path()) -> term().
get_raw(KeyPath) ->
    deep_get(KeyPath, get_raw()).

-spec get_raw(config_key_path(), term()) -> term().
get_raw(KeyPath, Default) ->
    deep_get(KeyPath, get_raw(), Default).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    persistent_term:put(?RAW_CONF, Config).

-spec put_raw(config_key_path(), term()) -> ok.
put_raw(KeyPath, Config) ->
    put_raw(deep_put(KeyPath, get_raw(), Config)).

%%-----------------------------------------------------------------
-dialyzer([{nowarn_function, [deep_get/2]}]).
-spec deep_get(config_key_path(), map()) -> term().
deep_get(ConfKeyPath, Map) ->
    do_deep_get(ConfKeyPath, Map, fun(KeyPath, Data) ->
        error({not_found, KeyPath, Data}) end).

-spec deep_get(config_key_path(), map(), term()) -> term().
deep_get(ConfKeyPath, Map, Default) ->
    do_deep_get(ConfKeyPath, Map, fun(_, _) -> Default end).

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

-spec do_deep_get(config_key_path(), map(), fun((config_key(), term()) -> any())) -> term().
do_deep_get([], Map, _) ->
    Map;
do_deep_get([Key | KeyPath], Map, OnNotFound) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} -> do_deep_get(KeyPath, SubMap, OnNotFound);
        error -> OnNotFound(Key, Map)
    end;
do_deep_get([Key | _KeyPath], Data, OnNotFound) ->
    OnNotFound(Key, Data).

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
