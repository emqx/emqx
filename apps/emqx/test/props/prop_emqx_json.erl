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

-module(prop_emqx_json).

-import(
    emqx_json,
    [
        decode/1,
        decode/2,
        encode/1,
        safe_decode/1,
        safe_decode/2,
        safe_encode/1
    ]
).

-include_lib("proper/include/proper.hrl").

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_json_basic() ->
    ?FORALL(
        T,
        json_basic(),
        begin
            {ok, J} = safe_encode(T),
            {ok, T} = safe_decode(J),
            T = decode(encode(T)),
            true
        end
    ).

prop_json_basic_atom() ->
    ?FORALL(
        T0,
        latin_atom(),
        begin
            T = atom_to_binary(T0, utf8),
            {ok, J} = safe_encode(T0),
            {ok, T} = safe_decode(J),
            T = decode(encode(T0)),
            true
        end
    ).

prop_object_proplist_to_proplist() ->
    ?FORALL(
        T,
        json_object(),
        begin
            {ok, J} = safe_encode(T),
            {ok, T} = safe_decode(J),
            T = decode(encode(T)),
            true
        end
    ).

prop_object_map_to_map() ->
    ?FORALL(
        T,
        json_object_map(),
        begin
            {ok, J} = safe_encode(T),
            {ok, T} = safe_decode(J, [return_maps]),
            T = decode(encode(T), [return_maps]),
            true
        end
    ).

%% The duplicated key will be overridden
prop_object_proplist_to_map() ->
    ?FORALL(
        T0,
        json_object(),
        begin
            T = to_map(T0),
            {ok, J} = safe_encode(T0),
            {ok, T} = safe_decode(J, [return_maps]),
            T = decode(encode(T0), [return_maps]),
            true
        end
    ).

prop_object_map_to_proplist() ->
    ?FORALL(
        T0,
        json_object_map(),
        begin
            %% jiffy encode a map with descending order, that is,
            %% it is opposite with maps traversal sequence
            %% see: the `to_list` implementation
            T = to_list(T0),
            {ok, J} = safe_encode(T0),
            {ok, T} = safe_decode(J),
            T = decode(encode(T0)),
            true
        end
    ).

prop_safe_encode() ->
    ?FORALL(
        T,
        invalid_json_term(),
        begin
            {error, _} = safe_encode(T),
            true
        end
    ).

prop_safe_decode() ->
    ?FORALL(
        T,
        invalid_json_str(),
        begin
            {error, _} = safe_decode(T),
            true
        end
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

to_map([{_, _} | _] = L) ->
    lists:foldl(
        fun({Name, Value}, Acc) ->
            Acc#{Name => to_map(Value)}
        end,
        #{},
        L
    );
to_map(L) when is_list(L) ->
    [to_map(E) || E <- L];
to_map(T) ->
    T.

to_list(L) when is_list(L) ->
    [to_list(E) || E <- L];
to_list(M) when is_map(M) ->
    maps:fold(
        fun(K, V, Acc) ->
            [{K, to_list(V)} | Acc]
        end,
        [],
        M
    );
to_list(T) ->
    T.

%%--------------------------------------------------------------------
%% Generators (https://tools.ietf.org/html/rfc8259)
%%--------------------------------------------------------------------

%% true, false, null, and number(), string()
json_basic() ->
    oneof([true, false, null, number(), json_string()]).

latin_atom() ->
    emqx_proper_types:limited_latin_atom().

json_string() -> utf8().

json_object() ->
    oneof([
        json_array_1(),
        json_object_1(),
        json_array_object_1(),
        json_array_2(),
        json_object_2(),
        json_array_object_2()
    ]).

json_object_map() ->
    ?LET(L, json_object(), to_map(L)).

json_array_1() ->
    list(json_basic()).

json_array_2() ->
    list([json_basic(), json_array_1()]).

json_object_1() ->
    list({json_key(), json_basic()}).

json_object_2() ->
    list({
        json_key(),
        oneof([
            json_basic(),
            json_array_1(),
            json_object_1()
        ])
    }).

json_array_object_1() ->
    list(json_object_1()).

json_array_object_2() ->
    list(json_object_2()).

%% @private
json_key() ->
    ?LET(K, latin_atom(), atom_to_binary(K, utf8)).

invalid_json_term() ->
    ?SUCHTHAT(T, tuple(), (tuple_size(T) /= 1)).

invalid_json_str() ->
    ?LET(T, json_object_2(), chaos(encode(T))).

%% @private
chaos(S) when is_binary(S) ->
    T = [$\r, $\n, $", ${, $}, $[, $], $:, $,],
    iolist_to_binary(chaos(binary_to_list(S), 100, T)).

chaos(S, 0, _) ->
    S;
chaos(S, N, T) ->
    I = rand:uniform(length(S)),
    {L1, L2} = lists:split(I, S),
    chaos(lists:flatten([L1, lists:nth(rand:uniform(length(T)), T), L2]), N - 1, T).
