%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_utils_maps).

-export([
    best_effort_recursive_sum/3,
    binary_key_map/1,
    binary_string/1,
    deep_convert/3,
    deep_find/2,
    deep_force_put/3,
    deep_get/2,
    deep_get/3,
    deep_merge/2,
    deep_put/3,
    deep_remove/2,
    diff_maps/2,
    if_only_to_toggle_enable/2,
    indent/3,
    jsonable_map/1,
    jsonable_map/2,
    key_comparer/1,
    put_if/4,
    rename/3,
    safe_atom_key_map/1,
    unindent/2,
    unsafe_atom_key_map/1,
    update_if_present/3
]).

-export_type([config_key/0, config_key_path/0]).
-type config_key() :: atom() | binary() | [byte()].
-type config_key_path() :: [config_key()].
-type convert_fun() :: fun((...) -> {K1 :: any(), V1 :: any()} | drop).

-define(CONFIG_NOT_FOUND_MAGIC, '$0tFound').
%%-----------------------------------------------------------------
-spec deep_get(config_key_path(), map()) -> term().
deep_get(ConfKeyPath, Map) ->
    case deep_get(ConfKeyPath, Map, ?CONFIG_NOT_FOUND_MAGIC) of
        ?CONFIG_NOT_FOUND_MAGIC -> error({config_not_found, ConfKeyPath});
        Res -> Res
    end.

-spec deep_get(config_key_path(), map(), term()) -> term().
deep_get(ConfKeyPath, Map, Default) ->
    case deep_find(ConfKeyPath, Map) of
        {not_found, _KeyPath, _Data} -> Default;
        {ok, Data} -> Data
    end.

-spec deep_find(config_key_path(), map()) ->
    {ok, term()} | {not_found, config_key_path(), term()}.
deep_find([], Map) ->
    {ok, Map};
deep_find([Key | KeyPath] = Path, Map) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} -> deep_find(KeyPath, SubMap);
        error -> {not_found, Path, Map}
    end;
deep_find(KeyPath, Data) ->
    {not_found, KeyPath, Data}.

-spec deep_put(config_key_path(), map(), term()) -> map().
deep_put([], _Map, Data) ->
    Data;
deep_put([Key | KeyPath], Map, Data) ->
    SubMap = maps:get(Key, Map, #{}),
    Map#{Key => deep_put(KeyPath, SubMap, Data)}.

%% Like deep_put, but ensures that the key path is present.
%% If key path is not present in map, creates the keys, until it's present
%% deep_force_put([x, y, z], #{a => 1}, 0) -> #{a => 1, x => #{y => #{z => 0}}}
-spec deep_force_put(config_key_path(), map(), term()) -> map().
deep_force_put([], _Map, Data) ->
    Data;
deep_force_put([Key | KeyPath] = FullPath, Map, Data) ->
    case Map of
        #{Key := InnerValue} ->
            Map#{Key => deep_force_put(KeyPath, InnerValue, Data)};
        #{} ->
            maps:put(Key, path_to_map(KeyPath, Data), Map);
        _ ->
            path_to_map(FullPath, Data)
    end.

-spec path_to_map(config_key_path(), term()) -> map().
path_to_map([], Data) -> Data;
path_to_map([Key | Tail], Data) -> #{Key => path_to_map(Tail, Data)}.

-spec deep_remove(config_key_path(), map()) -> map().
deep_remove([], Map) ->
    Map;
deep_remove([Key], Map) ->
    maps:remove(Key, Map);
deep_remove([Key | KeyPath], Map) ->
    case maps:find(Key, Map) of
        {ok, SubMap} when is_map(SubMap) ->
            Map#{Key => deep_remove(KeyPath, SubMap)};
        {ok, _Val} ->
            Map;
        error ->
            Map
    end.

%% #{a => #{b => 3, c => 2}, d => 4}
%%  = deep_merge(#{a => #{b => 1, c => 2}, d => 4}, #{a => #{b => 3}}).
-spec deep_merge(map(), map()) -> map().
deep_merge(BaseMap, NewMap) ->
    NewKeys = maps:keys(NewMap) -- maps:keys(BaseMap),
    MergedBase = maps:fold(
        fun(K, V, Acc) ->
            case maps:find(K, NewMap) of
                error ->
                    Acc#{K => V};
                {ok, NewV} when is_map(V), is_map(NewV) ->
                    Acc#{K => deep_merge(V, NewV)};
                {ok, NewV} ->
                    Acc#{K => NewV}
            end
        end,
        #{},
        BaseMap
    ),
    maps:merge(MergedBase, maps:with(NewKeys, NewMap)).

-spec deep_convert(any(), convert_fun(), Args :: list()) -> any().
deep_convert(Map, ConvFun, Args) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            case apply(ConvFun, [K, deep_convert(V, ConvFun, Args) | Args]) of
                drop -> Acc;
                {K1, V1} -> Acc#{K1 => V1}
            end
        end,
        #{},
        Map
    );
deep_convert(ListV, ConvFun, Args) when is_list(ListV) ->
    [deep_convert(V, ConvFun, Args) || V <- ListV];
deep_convert(Val, _, _Args) ->
    Val.

-spec unsafe_atom_key_map(#{binary() | atom() => any()}) -> #{atom() => any()}.
unsafe_atom_key_map(Map) ->
    convert_keys_to_atom(Map, fun(K) -> binary_to_atom(K, utf8) end).

-spec binary_key_map
    (map()) -> map();
    (list()) -> list().
binary_key_map(Map) ->
    deep_convert(
        Map,
        fun
            (K, V) when is_atom(K) -> {atom_to_binary(K, utf8), V};
            (K, V) when is_binary(K) -> {K, V}
        end,
        []
    ).

-spec safe_atom_key_map(#{binary() | atom() => any()}) -> #{atom() => any()}.
safe_atom_key_map(Map) ->
    convert_keys_to_atom(Map, fun(K) -> binary_to_existing_atom(K, utf8) end).

-spec jsonable_map(map() | list()) -> map() | list().
jsonable_map(Map) ->
    jsonable_map(Map, fun(K, V) -> {K, V} end).

jsonable_map(Map, JsonableFun) ->
    deep_convert(Map, fun binary_string_kv/3, [JsonableFun]).

-spec diff_maps(map(), map()) ->
    #{
        added := map(),
        identical := map(),
        removed := map(),
        changed := #{any() => {OldValue :: any(), NewValue :: any()}}
    }.
diff_maps(NewMap, OldMap) ->
    InitR = #{identical => #{}, changed => #{}, removed => #{}},
    {Result, RemInNew} =
        lists:foldl(
            fun({OldK, OldV}, {Result0 = #{identical := I, changed := U, removed := D}, RemNewMap}) ->
                Result1 =
                    case maps:find(OldK, NewMap) of
                        error ->
                            Result0#{removed => D#{OldK => OldV}};
                        {ok, NewV} when NewV == OldV ->
                            Result0#{identical => I#{OldK => OldV}};
                        {ok, NewV} ->
                            Result0#{changed => U#{OldK => {OldV, NewV}}}
                    end,
                {Result1, maps:remove(OldK, RemNewMap)}
            end,
            {InitR, NewMap},
            maps:to_list(OldMap)
        ),
    Result#{added => RemInNew}.

binary_string_kv(K, V, JsonableFun) ->
    case JsonableFun(K, V) of
        drop -> drop;
        {K1, V1} -> {binary_string(K1), V1}
    end.

%% [FIXME] this doesn't belong here
binary_string([]) ->
    [];
binary_string(Val) when is_list(Val) ->
    case io_lib:printable_unicode_list(Val) of
        true -> unicode:characters_to_binary(Val);
        false -> [binary_string(V) || V <- Val]
    end;
binary_string(Val) ->
    Val.

%%---------------------------------------------------------------------------
convert_keys_to_atom(BinKeyMap, Conv) ->
    deep_convert(
        BinKeyMap,
        fun
            (K, V) when is_atom(K) -> {K, V};
            (K, V) when is_binary(K) -> {Conv(K), V}
        end,
        []
    ).

%% @doc Sum-merge map values.
%% For bad merges, ErrorLogger is called to log the key, and value in M2 is ignored.
best_effort_recursive_sum(M10, M20, ErrorLogger) ->
    FilterF = fun(K, V) ->
        case erlang:is_number(V) of
            true ->
                true;
            false ->
                ErrorLogger(#{failed_to_merge => K, bad_value => V}),
                false
        end
    end,
    M1 = deep_filter(M10, FilterF),
    M2 = deep_filter(M20, FilterF),
    do_best_effort_recursive_sum(M1, M2, ErrorLogger).

do_best_effort_recursive_sum(M1, M2, ErrorLogger) ->
    F =
        fun(Key, V1, V2) ->
            case {erlang:is_map(V1), erlang:is_map(V2)} of
                {true, true} ->
                    do_best_effort_recursive_sum(V1, V2, ErrorLogger);
                {true, false} ->
                    ErrorLogger(#{failed_to_merge => Key, bad_value => V2}),
                    do_best_effort_recursive_sum(V1, #{}, ErrorLogger);
                {false, true} ->
                    ErrorLogger(#{failed_to_merge => Key, bad_value => V1}),
                    do_best_effort_recursive_sum(V2, #{}, ErrorLogger);
                {false, false} ->
                    true = is_number(V1),
                    true = is_number(V2),
                    V1 + V2
            end
        end,
    maps:merge_with(F, M1, M2).

deep_filter(M, F) when is_map(M) ->
    %% maps:filtermap is not available before OTP 24
    maps:from_list(
        lists:filtermap(
            fun
                ({K, V}) when is_map(V) ->
                    {true, {K, deep_filter(V, F)}};
                ({K, V}) ->
                    F(K, V) andalso {true, {K, V}}
            end,
            maps:to_list(M)
        )
    ).

if_only_to_toggle_enable(OldConf, Conf) ->
    #{added := Added, removed := Removed, changed := Updated} =
        emqx_utils_maps:diff_maps(OldConf, Conf),
    case {Added, Removed, Updated} of
        {Added, Removed, #{enable := _} = Updated} when
            map_size(Added) =:= 0,
            map_size(Removed) =:= 0,
            map_size(Updated) =:= 1
        ->
            true;
        {_, _, _} ->
            false
    end.

%% Like `maps:update_with', but does nothing if key does not exist.
update_if_present(Key, Fun, Map) ->
    case Map of
        #{Key := Val} ->
            Map#{Key := Fun(Val)};
        _ ->
            Map
    end.

put_if(Acc, K, V, true) ->
    Acc#{K => V};
put_if(Acc, _K, _V, false) ->
    Acc.

rename(OldKey, NewKey, Map) ->
    case maps:find(OldKey, Map) of
        {ok, Value} ->
            maps:put(NewKey, Value, maps:remove(OldKey, Map));
        error ->
            Map
    end.

-spec key_comparer(K) -> fun((M, M) -> boolean()) when M :: #{K => _V}.
key_comparer(K) ->
    fun
        (#{K := V1}, #{K := V2}) ->
            V1 < V2;
        (#{K := _}, _) ->
            false;
        (_, #{K := _}) ->
            true;
        (M1, M2) ->
            M1 < M2
    end.

-spec indent(term(), [term()], map()) -> map().
indent(IndentKey, PickKeys, Map) ->
    maps:put(
        IndentKey,
        maps:with(PickKeys, Map),
        maps:without(PickKeys, Map)
    ).

-spec unindent(term(), map()) -> map().
unindent(Key, Map) ->
    deep_merge(
        maps:remove(Key, Map),
        maps:get(Key, Map, #{})
    ).
