%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_json_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-import(
    emqx_utils_json,
    [
        encode/1,
        decode/1,
        decode/2,
        decode/2,
        encode_proplist/1,
        decode_proplist/1
    ]
).

%% copied jiffy/readme
%%--------------------------------------------------------------------
%% Erlang                     JSON              Erlang
%% -------------------------------------------------------------------
%%
%% null                       -> null           -> null
%% true                       -> true           -> true
%% false                      -> false          -> false
%% "hi"                       -> [104, 105]     -> [104, 105]
%% <<"hi">>                   -> "hi"           -> <<"hi">>
%% hi                         -> "hi"           -> <<"hi">>
%% 1                          -> 1              -> 1
%% 1.25                       -> 1.25           -> 1.25
%% []                         -> []             -> []
%% [true, 1.0]                -> [true, 1.0]    -> [true, 1.0]
%% {[]}                       -> {}             -> {[]}
%% {[{foo, bar}]}             -> {"foo": "bar"} -> {[{<<"foo">>, <<"bar">>}]}
%% {[{<<"foo">>, <<"bar">>}]} -> {"foo": "bar"} -> {[{<<"foo">>, <<"bar">>}]}
%% #{<<"foo">> => <<"bar">>}  -> {"foo": "bar"} -> #{<<"foo">> => <<"bar">>}
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

t_decode_encode(_) ->
    null = decode(encode(null)),
    true = decode(encode(true)),
    false = decode(encode(false)),
    "hi" = decode(encode("hi")),
    <<"hi">> = decode(encode(hi)),
    1 = decode(encode(1)),
    1.25 = decode(encode(1.25)),
    [] = decode(encode([])),
    [true, 1] = decode(encode([true, 1])),
    #{} = decode(encode({[]})),
    {[{<<"foo">>, <<"bar">>}]} = decode(encode({[{foo, bar}]}), []),
    {[{<<"foo">>, <<"bar">>}]} = decode(encode({[{<<"foo">>, <<"bar">>}]}), []),
    #{<<"foo">> := <<"bar">>} = decode(encode({[{<<"foo">>, <<"bar">>}]})),
    #{<<"foo">> := <<"bar">>} = decode(encode(#{<<"foo">> => <<"bar">>})),
    JsonText = <<"{\"bool\":true,\"int\":10,\"foo\":\"bar\"}">>,
    JsonMaps = #{
        <<"bool">> => true,
        <<"int">> => 10,
        <<"foo">> => <<"bar">>
    },
    ?assertEqual(JsonText, encode(decode(JsonText, []))),
    ?assertEqual(JsonMaps, decode(JsonText)),
    ?assertEqual(JsonMaps, decode(JsonText, [return_maps])),
    ?assertEqual(
        #{<<"foo">> => #{<<"bar">> => <<"baz">>}},
        decode(encode(#{<<"foo">> => {[{<<"bar">>, <<"baz">>}]}}))
    ).

t_decode_encode_proplist(_) ->
    [] = decode_proplist(encode_proplist([])),
    [] = decode_proplist(encode_proplist(#{})),
    [{<<"a">>, <<"foo">>}, {<<"b">>, <<"bar">>}] =
        decode_proplist(encode_proplist([{a, <<"foo">>}, {b, <<"bar">>}])),
    [[{<<"foo">>, <<"bar">>}]] =
        decode_proplist(encode_proplist([[{<<"foo">>, <<"bar">>}]])),
    [
        <<"string">>,
        [{<<"a">>, <<"b">>}],
        [{<<"x">>, <<"y">>}]
    ] = decode_proplist(
        encode_proplist([
            string,
            [{<<"a">>, <<"b">>}],
            [{<<"x">>, <<"y">>}]
        ])
    ).

t_safe_decode_encode(_) ->
    safe_encode_decode(null),
    safe_encode_decode(true),
    safe_encode_decode(false),
    "hi" = safe_encode_decode("hi"),
    <<"hi">> = safe_encode_decode(hi),
    1 = safe_encode_decode(1),
    1.25 = safe_encode_decode(1.25),
    [] = safe_encode_decode([]),
    [true, 1] = safe_encode_decode([true, 1]),
    {[]} = safe_encode_decode({[]}, []),
    #{} = safe_encode_decode({[]}),
    {[{<<"foo">>, <<"bar">>}]} = safe_encode_decode({[{foo, bar}]}, []),
    #{<<"foo">> := <<"bar">>} = safe_encode_decode({[{<<"foo">>, <<"bar">>}]}),
    {ok, Json} = emqx_utils_json:safe_encode(#{<<"foo">> => <<"bar">>}),
    {ok, #{<<"foo">> := <<"bar">>}} = emqx_utils_json:safe_decode(Json),
    {ok, #{<<"foo">> := <<"bar">>}} = emqx_utils_json:safe_decode(Json, [return_maps]).

safe_encode_decode(Term) ->
    {ok, Json} = emqx_utils_json:safe_encode(Term),
    {ok, NTerm} = emqx_utils_json:safe_decode(Json),
    NTerm.

safe_encode_decode(Term, Opts) ->
    {ok, Json} = emqx_utils_json:safe_encode(Term),
    {ok, NTerm} = emqx_utils_json:safe_decode(Json, Opts),
    NTerm.

t_is_json(_) ->
    ?assert(emqx_utils_json:is_json(<<"{}">>)),
    ?assert(not emqx_utils_json:is_json(<<"foo">>)).
