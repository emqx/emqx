%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    deep_get/2,
    deep_get/3,
    deep_find/2,
    deep_put/3,
    deep_force_put/3,
    deep_remove/2,
    deep_merge/2,
    binary_key_map/1,
    safe_atom_key_map/1,
    unsafe_atom_key_map/1,
    jsonable_map/1,
    jsonable_map/2,
    binary_string/1,
    deep_convert/3,
    diff_maps/2,
    merge_with/3,
    best_effort_recursive_sum/3,
    if_only_to_toggle_enable/2
]).

-export_type([config_key/0, config_key_path/0]).
-type config_key() :: atom() | binary() | [byte()].
-type config_key_path() :: [config_key()].
-type convert_fun() :: fun((...) -> {K1 :: any(), V1 :: any()} | drop).

%%-----------------------------------------------------------------
-spec deep_get(config_key_path(), map()) -> term().
deep_get(ConfKeyPath, Map) ->
    Ref = make_ref(),
    Res = deep_get(ConfKeyPath, Map, Ref),
    case Res =:= Ref of
        true -> error({config_not_found, ConfKeyPath});
        false -> Res
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
    covert_keys_to_atom(Map, fun(K) -> binary_to_atom(K, utf8) end).

-spec binary_key_map(map()) -> map().
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
    covert_keys_to_atom(Map, fun(K) -> binary_to_existing_atom(K, utf8) end).

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
covert_keys_to_atom(BinKeyMap, Conv) ->
    deep_convert(
        BinKeyMap,
        fun
            (K, V) when is_atom(K) -> {K, V};
            (K, V) when is_binary(K) -> {Conv(K), V}
        end,
        []
    ).

%% copy from maps.erl OTP24.0
merge_with(Combiner, Map1, Map2) when
    is_map(Map1),
    is_map(Map2),
    is_function(Combiner, 3)
->
    case map_size(Map1) > map_size(Map2) of
        true ->
            Iterator = maps:iterator(Map2),
            merge_with_t(
                maps:next(Iterator),
                Map1,
                Map2,
                Combiner
            );
        false ->
            Iterator = maps:iterator(Map1),
            merge_with_t(
                maps:next(Iterator),
                Map2,
                Map1,
                fun(K, V1, V2) -> Combiner(K, V2, V1) end
            )
    end;
merge_with(Combiner, Map1, Map2) ->
    ErrorType = error_type_merge_intersect(Map1, Map2, Combiner),
    throw(#{maps_merge_error => ErrorType, args => [Map1, Map2]}).

merge_with_t({K, V2, Iterator}, Map1, Map2, Combiner) ->
    case Map1 of
        #{K := V1} ->
            NewMap1 = Map1#{K := Combiner(K, V1, V2)},
            merge_with_t(maps:next(Iterator), NewMap1, Map2, Combiner);
        #{} ->
            merge_with_t(maps:next(Iterator), maps:put(K, V2, Map1), Map2, Combiner)
    end;
merge_with_t(none, Result, _, _) ->
    Result.

error_type_merge_intersect(M1, M2, Combiner) when is_function(Combiner, 3) ->
    error_type_two_maps(M1, M2);
error_type_merge_intersect(_M1, _M2, _Combiner) ->
    badarg_combiner_function.

error_type_two_maps(M1, M2) when is_map(M1) ->
    {badmap, M2};
error_type_two_maps(M1, _M2) ->
    {badmap, M1}.

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
    merge_with(F, M1, M2).

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
        emqx_map_lib:diff_maps(OldConf, Conf),
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
