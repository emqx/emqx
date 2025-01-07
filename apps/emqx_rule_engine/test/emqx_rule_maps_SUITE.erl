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

-module(emqx_rule_maps_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_rule_maps,
    [
        nested_get/2,
        nested_get/3,
        nested_put/3,
        atom_key_map/1
    ]
).

-define(path(Path),
    {path, [
        case K of
            {ic, Key} -> {index, {const, Key}};
            {iv, Key} -> {index, {var, Key}};
            {i, Path1} -> {index, Path1};
            _ -> {key, K}
        end
     || K <- Path
    ]}
).

-define(PROPTEST(Prop), true = proper:quickcheck(Prop)).

t_nested_put_map(_) ->
    ?assertEqual(#{a => 1}, nested_put(?path([a]), 1, #{})),
    ?assertEqual(#{a => a}, nested_put(?path([a]), a, #{})),
    ?assertEqual(#{a => undefined}, nested_put(?path([a]), undefined, #{})),
    ?assertEqual(#{a => 1}, nested_put(?path([a]), 1, not_map)),
    ?assertEqual(#{a => #{b => b}}, nested_put(?path([a, b]), b, #{})),
    ?assertEqual(#{a => #{b => #{c => c}}}, nested_put(?path([a, b, c]), c, #{})),
    ?assertEqual(#{<<"k">> => v1}, nested_put(?path([k]), v1, #{<<"k">> => v0})),
    ?assertEqual(#{k => v1}, nested_put(?path([k]), v1, #{k => v0})),
    ?assertEqual(#{<<"k">> => v1, a => b}, nested_put(?path([k]), v1, #{<<"k">> => v0, a => b})),
    ?assertEqual(#{<<"k">> => v1}, nested_put(?path([k]), v1, #{<<"k">> => v0})),
    ?assertEqual(#{k => v1}, nested_put(?path([k]), v1, #{k => v0})),
    ?assertEqual(#{k => v1, a => b}, nested_put(?path([k]), v1, #{k => v0, a => b})),
    ?assertEqual(#{<<"k">> => v1, a => b}, nested_put(?path([k]), v1, #{<<"k">> => v0, a => b})),
    ?assertEqual(
        #{<<"k">> => #{<<"t">> => v1}},
        nested_put(?path([k, t]), v1, #{<<"k">> => #{<<"t">> => v0}})
    ),
    ?assertEqual(#{<<"k">> => #{t => v1}}, nested_put(?path([k, t]), v1, #{<<"k">> => #{t => v0}})),
    ?assertEqual(
        #{k => #{<<"t">> => #{a => v1}}}, nested_put(?path([k, t, a]), v1, #{k => #{<<"t">> => v0}})
    ),
    ?assertEqual(
        #{k => #{<<"t">> => #{<<"a">> => v1}}},
        nested_put(?path([k, t, <<"a">>]), v1, #{k => #{<<"t">> => v0}})
    ),
    %% note: since we handle json-encoded binaries when evaluating the
    %% rule rather than baking the decoding in `nested_put`, we test
    %% this corner case that _would_ otherwise lose data to
    %% demonstrate this behavior.
    ?assertEqual(
        #{payload => #{<<"a">> => v1}},
        nested_put(
            ?path([payload, <<"a">>]),
            v1,
            #{payload => emqx_utils_json:encode(#{b => <<"v2">>})}
        )
    ),
    %% We have an asymmetry in the behavior here because `nested_put'
    %% currently, at each key, will use `general_find' to get the
    %% current value of the eky, and that attempts JSON decoding the
    %% such value...  So, the cases below, `old' gets preserved
    %% because it's in this direct path.
    ?assertEqual(
        #{payload => #{<<"a">> => #{<<"new">> => v1, <<"old">> => <<"v2">>}}},
        nested_put(
            ?path([payload, <<"a">>, <<"new">>]),
            v1,
            #{payload => emqx_utils_json:encode(#{a => #{old => <<"v2">>}})}
        )
    ),
    ?assertEqual(
        #{payload => #{<<"a">> => #{<<"new">> => v1, <<"old">> => <<"{}">>}}},
        nested_put(
            ?path([payload, <<"a">>, <<"new">>]),
            v1,
            #{payload => emqx_utils_json:encode(#{a => #{old => <<"{}">>}, b => <<"{}">>})}
        )
    ),
    ?assertEqual(
        #{payload => #{<<"a">> => #{<<"new">> => v1}}},
        nested_put(
            ?path([payload, <<"a">>, <<"new">>]),
            v1,
            #{payload => <<"{}">>}
        )
    ),
    ok.

t_nested_put_index(_) ->
    ?assertEqual([1, a, 3], nested_put(?path([{ic, 2}]), a, [1, 2, 3])),
    ?assertEqual([1, 2, 3], nested_put(?path([{ic, 0}]), a, [1, 2, 3])),
    ?assertEqual([1, 2, 3], nested_put(?path([{ic, 4}]), a, [1, 2, 3])),
    ?assertEqual([1, [a], 3], nested_put(?path([{ic, 2}, {ic, 1}]), a, [1, [2], 3])),
    ?assertEqual([1, [[a]], 3], nested_put(?path([{ic, 2}, {ic, 1}, {ic, 1}]), a, [1, [[2]], 3])),
    ?assertEqual([1, [[2]], 3], nested_put(?path([{ic, 2}, {ic, 1}, {ic, 2}]), a, [1, [[2]], 3])),
    ?assertEqual([1, [a], 1], nested_put(?path([{ic, 2}, {i, ?path([{ic, 3}])}]), a, [1, [2], 1])),
    %% nested_put to the first or tail of a list:
    ?assertEqual([a], nested_put(?path([{ic, head}]), a, not_list)),
    ?assertEqual([a], nested_put(?path([{ic, head}]), a, [])),
    ?assertEqual([a, 1, 2, 3], nested_put(?path([{ic, head}]), a, [1, 2, 3])),
    ?assertEqual([a], nested_put(?path([{ic, tail}]), a, not_list)),
    ?assertEqual([a], nested_put(?path([{ic, tail}]), a, [])),
    ?assertEqual([1, 2, 3, a], nested_put(?path([{ic, tail}]), a, [1, 2, 3])).

t_nested_put_negative_index(_) ->
    ?assertEqual([1, 2, a], nested_put(?path([{ic, -1}]), a, [1, 2, 3])),
    ?assertEqual([1, a, 3], nested_put(?path([{ic, -2}]), a, [1, 2, 3])),
    ?assertEqual([a, 2, 3], nested_put(?path([{ic, -3}]), a, [1, 2, 3])),
    ?assertEqual([1, 2, 3], nested_put(?path([{ic, -4}]), a, [1, 2, 3])).

t_nested_put_mix_map_index(_) ->
    ?assertEqual(#{a => [a]}, nested_put(?path([a, {ic, 2}]), a, #{})),
    ?assertEqual(#{a => [#{b => 0}]}, nested_put(?path([a, {ic, 2}, b]), 0, #{})),
    ?assertEqual(#{a => [1, a, 3]}, nested_put(?path([a, {ic, 2}]), a, #{a => [1, 2, 3]})),
    ?assertEqual([1, #{a => c}, 3], nested_put(?path([{ic, 2}, a]), c, [1, #{a => b}, 3])),
    ?assertEqual(
        [1, #{a => [c]}, 3], nested_put(?path([{ic, 2}, a, {ic, 1}]), c, [1, #{a => [b]}, 3])
    ),
    ?assertEqual(
        #{a => [1, a, 3], b => 2}, nested_put(?path([a, {iv, b}]), a, #{a => [1, 2, 3], b => 2})
    ),
    ?assertEqual(
        #{a => [1, 2, 3], b => 2}, nested_put(?path([a, {iv, c}]), a, #{a => [1, 2, 3], b => 2})
    ),
    ?assertEqual(
        #{a => [#{c => a}, 1, 2, 3]}, nested_put(?path([a, {ic, head}, c]), a, #{a => [1, 2, 3]})
    ).

t_nested_get_map(_) ->
    ?assertEqual(undefined, nested_get(?path([a]), not_map)),
    ?assertEqual(#{a => 1}, nested_get(?path([]), #{a => 1})),
    ?assertEqual(#{b => c}, nested_get(?path([a]), #{a => #{b => c}})),
    ?assertEqual(undefined, nested_get(?path([a, b, c]), not_map)),
    ?assertEqual(undefined, nested_get(?path([a, b, c]), #{})),
    ?assertEqual(undefined, nested_get(?path([a, b, c]), #{a => #{}})),
    ?assertEqual(undefined, nested_get(?path([a, b, c]), #{a => #{b => #{}}})),
    ?assertEqual(v1, nested_get(?path([p, x]), #{p => #{x => v1}})),
    ?assertEqual(v1, nested_get(?path([<<"p">>, <<"x">>]), #{p => #{x => v1}})),
    ?assertEqual(c, nested_get(?path([a, b, c]), #{a => #{b => #{c => c}}})).

t_nested_get_map_1(_) ->
    ?assertEqual(1, nested_get(?path([a]), <<"{\"a\": 1}">>)),
    ?assertEqual(<<"{\"b\": 1}">>, nested_get(?path([a]), #{a => <<"{\"b\": 1}">>})),
    ?assertEqual(1, nested_get(?path([a, b]), #{a => <<"{\"b\": 1}">>})).

t_nested_get_index(_) ->
    %% single index get
    ?assertEqual(1, nested_get(?path([{ic, 1}]), [1, 2, 3])),
    ?assertEqual(2, nested_get(?path([{ic, 2}]), [1, 2, 3])),
    ?assertEqual(3, nested_get(?path([{ic, 3}]), [1, 2, 3])),
    ?assertEqual(undefined, nested_get(?path([{ic, 0}]), [1, 2, 3])),
    ?assertEqual("not_found", nested_get(?path([{ic, 0}]), [1, 2, 3], "not_found")),
    ?assertEqual(undefined, nested_get(?path([{ic, 4}]), [1, 2, 3])),
    ?assertEqual("not_found", nested_get(?path([{ic, 4}]), [1, 2, 3], "not_found")),
    %% multiple index get
    ?assertEqual(c, nested_get(?path([{ic, 2}, {ic, 3}]), [1, [a, b, c], 3])),
    ?assertEqual(
        "I", nested_get(?path([{ic, 2}, {ic, 3}, {ic, 1}]), [1, [a, b, ["I", "II", "III"]], 3])
    ),
    ?assertEqual(
        undefined,
        nested_get(?path([{ic, 2}, {ic, 1}, {ic, 1}]), [1, [a, b, ["I", "II", "III"]], 3])
    ),
    ?assertEqual(
        default,
        nested_get(?path([{ic, 2}, {ic, 1}, {ic, 1}]), [1, [a, b, ["I", "II", "III"]], 3], default)
    ).

t_nested_get_negative_index(_) ->
    ?assertEqual(3, nested_get(?path([{ic, -1}]), [1, 2, 3])),
    ?assertEqual(2, nested_get(?path([{ic, -2}]), [1, 2, 3])),
    ?assertEqual(1, nested_get(?path([{ic, -3}]), [1, 2, 3])),
    ?assertEqual(undefined, nested_get(?path([{ic, -4}]), [1, 2, 3])).

t_nested_get_mix_map_index(_) ->
    %% index const
    ?assertEqual(1, nested_get(?path([a, {ic, 1}]), #{a => [1, 2, 3]})),
    ?assertEqual(2, nested_get(?path([{ic, 2}, a]), [1, #{a => 2}, 3])),
    ?assertEqual(undefined, nested_get(?path([a, {ic, 0}]), #{a => [1, 2, 3]})),
    ?assertEqual("not_found", nested_get(?path([a, {ic, 0}]), #{a => [1, 2, 3]}, "not_found")),
    ?assertEqual("not_found", nested_get(?path([b, {ic, 1}]), #{a => [1, 2, 3]}, "not_found")),
    ?assertEqual(undefined, nested_get(?path([{ic, 4}, a]), [1, 2, 3, 4])),
    ?assertEqual("not_found", nested_get(?path([{ic, 4}, a]), [1, 2, 3, 4], "not_found")),
    ?assertEqual(c, nested_get(?path([a, {ic, 2}, {ic, 3}]), #{a => [1, [a, b, c], 3]})),
    ?assertEqual(
        "I",
        nested_get(?path([{ic, 2}, c, {ic, 1}]), [1, #{a => a, b => b, c => ["I", "II", "III"]}, 3])
    ),
    ?assertEqual(
        "I", nested_get(?path([{ic, 2}, c, d]), [1, #{a => a, b => b, c => #{d => "I"}}, 3])
    ),
    ?assertEqual(
        undefined, nested_get(?path([{ic, 2}, c, e]), [1, #{a => a, b => b, c => #{d => "I"}}, 3])
    ),
    ?assertEqual(
        default,
        nested_get(?path([{ic, 2}, c, e]), [1, #{a => a, b => b, c => #{d => "I"}}, 3], default)
    ),
    %% index var
    ?assertEqual(1, nested_get(?path([a, {iv, <<"b">>}]), #{a => [1, 2, 3], b => 1})),
    ?assertEqual(1, nested_get(?path([a, {iv, b}]), #{a => [1, 2, 3], b => 1})),
    ?assertEqual(undefined, nested_get(?path([a, {iv, c}]), #{a => [1, 2, 3], b => 1})),
    ?assertEqual(undefined, nested_get(?path([a, {iv, b}]), #{a => [1, 2, 3], b => 4})),
    ?assertEqual(
        "I",
        nested_get(
            ?path([{i, ?path([{ic, 3}])}, c, d]),
            [1, #{a => a, b => b, c => #{d => "I"}}, 2],
            default
        )
    ),
    ?assertEqual(
        3,
        nested_get(
            ?path([a, {i, ?path([b, {ic, 1}, c])}]),
            #{a => [1, 2, 3], b => [#{c => 3}]}
        )
    ),
    ?assertEqual(
        3,
        nested_get(
            ?path([a, {i, ?path([b, {ic, 1}, c])}]),
            #{a => [1, 2, 3], b => [#{c => 3}]},
            default
        )
    ),
    ?assertEqual(
        default,
        nested_get(
            ?path([a, {i, ?path([b, {ic, 1}, c])}]),
            #{a => [1, 2, 3], b => [#{c => 4}]},
            default
        )
    ),
    ?assertEqual(
        default,
        nested_get(
            ?path([a, {i, ?path([b, {ic, 2}, c])}]),
            #{a => [1, 2, 3], b => [#{c => 3}]},
            default
        )
    ).

t_atom_key_map(_) ->
    ?assertEqual(#{a => 1}, atom_key_map(#{<<"a">> => 1})),
    ?assertEqual(
        #{a => 1, b => #{a => 2}},
        atom_key_map(#{<<"a">> => 1, <<"b">> => #{<<"a">> => 2}})
    ),
    ?assertEqual(
        [#{a => 1}, #{b => #{a => 2}}],
        atom_key_map([#{<<"a">> => 1}, #{<<"b">> => #{<<"a">> => 2}}])
    ),
    ?assertEqual(
        #{a => 1, b => [#{a => 2}, #{c => 2}]},
        atom_key_map(#{<<"a">> => 1, <<"b">> => [#{<<"a">> => 2}, #{<<"c">> => 2}]})
    ).

all() ->
    IsTestCase = fun
        ("t_" ++ _) -> true;
        (_) -> false
    end,
    [F || {F, _A} <- module_info(exports), IsTestCase(atom_to_list(F))].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].
