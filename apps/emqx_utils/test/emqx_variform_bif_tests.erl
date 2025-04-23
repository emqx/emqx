%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Most of the functions are tested as rule-engine string funcs
-module(emqx_variform_bif_tests).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

regex_extract_test_() ->
    [
        ?_assertEqual([<<"12345">>], regex_extract("Order number: 12345", "(\\d+)")),
        ?_assertEqual(
            [<<"Hello">>, <<"world">>], regex_extract("Hello, world!", "(\\w+).*\s(\\w+)")
        ),
        ?_assertEqual([], regex_extract("No numbers here!", "(\\d+)")),
        ?_assertEqual(
            [<<"2021">>, <<"05">>, <<"20">>],
            regex_extract("Date: 2021-05-20", "(\\d{4})-(\\d{2})-(\\d{2})")
        ),
        ?_assertEqual([<<"Hello">>], regex_extract("Hello, world!", "(Hello)")),
        ?_assertEqual(
            [<<"12">>, <<"34">>], regex_extract("Items: 12, Price: 34", "(\\d+).*\s(\\d+)")
        ),
        ?_assertEqual(
            [<<"john.doe@example.com">>],
            regex_extract("Contact: john.doe@example.com", "([\\w\\.]+@[\\w\\.]+)")
        ),
        ?_assertEqual([], regex_extract("Just some text, nothing more.", "([A-Z]\\d{3})")),
        ?_assertEqual(
            [<<"admin">>, <<"1234">>],
            regex_extract("User: admin, Pass: 1234", "User: (\\w+), Pass: (\\d+)")
        ),
        ?_assertEqual([], regex_extract("", "(\\d+)")),
        ?_assertEqual([], regex_extract("$$$###!!!", "(\\d+)")),
        ?_assertEqual([<<"23.1">>], regex_extract("Erlang 23.1 version", "(\\d+\\.\\d+)")),
        ?_assertEqual(
            [<<"192.168.1.1">>],
            regex_extract("Server IP: 192.168.1.1 at port 8080", "(\\d+\\.\\d+\\.\\d+\\.\\d+)")
        )
    ].

regex_extract(Str, RegEx) ->
    emqx_variform_bif:regex_extract(Str, RegEx).

rand_str_test() ->
    ?assertEqual(3, size(emqx_variform_bif:rand_str(3))),
    ?assertThrow(#{reason := badarg}, size(emqx_variform_bif:rand_str(0))).

rand_int_test() ->
    N = emqx_variform_bif:rand_int(10),
    ?assert(N =< 10 andalso N >= 1),
    ?assertThrow(#{reason := badarg}, emqx_variform_bif:rand_int(0)),
    ?assertThrow(#{reason := badarg}, emqx_variform_bif:rand_int(-1)).

base64_encode_decode_test() ->
    RandBytes = crypto:strong_rand_bytes(100),
    Encoded = emqx_variform_bif:base64_encode(RandBytes),
    ?assertEqual(RandBytes, emqx_variform_bif:base64_decode(Encoded)).

system_test() ->
    EnvName = erlang:atom_to_list(?MODULE),
    EnvVal = erlang:atom_to_list(?FUNCTION_NAME),
    EnvNameBin = erlang:list_to_binary(EnvName),
    os:putenv("EMQXVAR_" ++ EnvName, EnvVal),
    ?assertEqual(erlang:list_to_binary(EnvVal), emqx_variform_bif:getenv(EnvNameBin)).

empty_val_test_() ->
    F = fun(X) -> emqx_variform_bif:is_empty_val(X) end,
    [
        ?_assert(F(undefined)),
        ?_assert(F(null)),
        ?_assert(F(<<>>)),
        ?_assert(F([])),
        ?_assertNot(F(true)),
        ?_assertNot(F(false)),
        ?_assertNot(F(<<"a">>))
    ].

bool_not_test_() ->
    Not = fun(X) -> emqx_variform_bif:'not'(X) end,
    [
        ?_assertEqual(<<"false">>, Not(<<"true">>)),
        ?_assertEqual(<<"true">>, Not(<<"false">>)),
        ?_assertEqual(true, Not(false)),
        ?_assertEqual(false, Not(true))
    ].

-define(ASSERT_BADARG(EXPR), ?_assertThrow(#{reason := badarg}, EXPR)).
null_badarg_test_() ->
    [
        ?ASSERT_BADARG(emqx_variform_bif:lower(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:lower(null)),
        ?ASSERT_BADARG(emqx_variform_bif:ltrim(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:ltrim(null)),
        ?ASSERT_BADARG(emqx_variform_bif:ltrim(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:ltrim(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:reverse(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:reverse(null)),
        ?ASSERT_BADARG(emqx_variform_bif:rtrim(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:rtrim(null)),
        ?ASSERT_BADARG(emqx_variform_bif:rtrim(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:rtrim(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:rm_prefix(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:rm_prefix(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:strlen(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:strlen(null)),
        ?ASSERT_BADARG(emqx_variform_bif:substr(undefined, 1)),
        ?ASSERT_BADARG(emqx_variform_bif:substr(null, 1)),
        ?ASSERT_BADARG(emqx_variform_bif:substr(undefined, 1, 2)),
        ?ASSERT_BADARG(emqx_variform_bif:substr(null, 1, 2)),
        ?ASSERT_BADARG(emqx_variform_bif:trim(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:trim(null)),
        ?ASSERT_BADARG(emqx_variform_bif:trim(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:trim(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:upper(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:upper(null)),
        ?ASSERT_BADARG(emqx_variform_bif:split(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:split(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:split(undefined, <<"a">>, <<"notrim">>)),
        ?ASSERT_BADARG(emqx_variform_bif:split(null, <<"a">>, <<"notrim">>)),
        ?ASSERT_BADARG(emqx_variform_bif:tokens(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:tokens(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:tokens(undefined, <<"a">>, <<"nocrlf">>)),
        ?ASSERT_BADARG(emqx_variform_bif:tokens(null, <<"a">>, <<"nocrlf">>)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(undefined, 1)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(null, 1)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(undefined, 1, <<"trailing">>)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(null, 1, <<"trailing">>)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(undefined, 1, <<"trailing">>, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:pad(null, 1, <<"trailing">>, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:replace(undefined, <<"a">>, <<"b">>)),
        ?ASSERT_BADARG(emqx_variform_bif:replace(null, <<"a">>, <<"b">>)),
        ?ASSERT_BADARG(emqx_variform_bif:replace(undefined, <<"a">>, <<"b">>, <<"all">>)),
        ?ASSERT_BADARG(emqx_variform_bif:replace(null, <<"a">>, <<"b">>, <<"all">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_match(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_match(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_replace(undefined, <<"a">>, <<"b">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_replace(null, <<"a">>, <<"b">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_extract(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:regex_extract(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:ascii(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:ascii(null)),
        ?ASSERT_BADARG(emqx_variform_bif:find(undefined, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:find(null, <<"a">>)),
        ?ASSERT_BADARG(emqx_variform_bif:find(undefined, <<"a">>, <<"trailing">>)),
        ?ASSERT_BADARG(emqx_variform_bif:find(null, <<"a">>, <<"trailing">>)),
        ?ASSERT_BADARG(emqx_variform_bif:unescape(undefined)),
        ?ASSERT_BADARG(emqx_variform_bif:unescape(null)),
        ?ASSERT_BADARG(emqx_variform_bif:hash_to_range(undefined, 1, 2)),
        ?ASSERT_BADARG(emqx_variform_bif:hash_to_range(null, 1, 2)),
        ?ASSERT_BADARG(emqx_variform_bif:map_to_range(undefined, 1, 10)),
        ?ASSERT_BADARG(emqx_variform_bif:map_to_range(null, 1, 10)),
        ?ASSERT_BADARG(emqx_variform_bif:hash(<<"sha1">>, null))
    ].

invalid_hash_algorithm_test() ->
    ?assertThrow(
        #{reason := unknown_hash_algorithm, algorithm := <<"unknown_algorithm">>},
        emqx_variform_bif:hash(<<"unknown_algorithm">>, <<"a">>)
    ).

int2hexstr_test() ->
    ?assertEqual(<<"0">>, emqx_variform_bif:int2hexstr(0)),
    ?assertEqual(<<"1">>, emqx_variform_bif:int2hexstr(1)),
    ?assertEqual(<<"A">>, emqx_variform_bif:int2hexstr(10)),
    ?assertEqual(<<"F">>, emqx_variform_bif:int2hexstr(15)),
    ?assertEqual(<<"10">>, emqx_variform_bif:int2hexstr(16)),
    ?assertEqual(<<"1A">>, emqx_variform_bif:int2hexstr(26)).

atom_input_test_() ->
    [
        ?_assertEqual(<<"ATOM">>, emqx_variform_bif:upper('atom')),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:lower('ATOM')),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:ltrim('atom')),
        ?_assertEqual(<<"tom">>, emqx_variform_bif:ltrim('atom', <<"a">>)),
        ?_assertEqual(<<"mota">>, emqx_variform_bif:reverse('atom')),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:rtrim('atom')),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:rtrim('atom', <<"a">>)),
        ?_assertEqual(<<"tom">>, emqx_variform_bif:rm_prefix('atom', <<"a">>)),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:rm_prefix('atom', <<"x">>)),
        ?_assertEqual(4, emqx_variform_bif:strlen('atom')),
        ?_assertEqual(<<"tom">>, emqx_variform_bif:substr('atom', 1)),
        ?_assertEqual(<<"to">>, emqx_variform_bif:substr('atom', 1, 2)),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:trim('atom')),
        ?_assertEqual(<<"tom">>, emqx_variform_bif:trim('atom', <<"a">>)),
        ?_assertEqual([<<"tom">>], emqx_variform_bif:split('atom', <<"a">>)),
        ?_assertEqual([<<>>, <<"tom">>], emqx_variform_bif:split('atom', <<"a">>, <<"notrim">>)),
        ?_assertEqual([<<"tom">>], emqx_variform_bif:tokens('atom', <<"a">>)),
        ?_assertEqual(<<"atom ">>, emqx_variform_bif:pad('atom', 5, <<"trailing">>)),
        ?_assertEqual(<<"atomx">>, emqx_variform_bif:pad('atom', 5, <<"trailing">>, <<"x">>)),
        ?_assertEqual(<<"btom">>, emqx_variform_bif:replace('atom', <<"a">>, <<"b">>)),
        ?_assertEqual(<<"btom">>, emqx_variform_bif:replace('atom', <<"a">>, <<"b">>, <<"all">>)),
        ?_assertEqual(true, emqx_variform_bif:regex_match('atom', <<"^atom$">>)),
        ?_assertEqual(<<"btom">>, emqx_variform_bif:regex_replace('atom', <<"a">>, <<"b">>)),
        ?_assertEqual([<<"atom">>], emqx_variform_bif:regex_extract('atom', <<"(atom)">>)),
        ?_assertEqual(97, emqx_variform_bif:ascii(a)),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:find('atom', <<"a">>)),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:find('atom', <<"a">>, <<"trailing">>)),
        ?_assertEqual(<<"atom">>, emqx_variform_bif:unescape('atom')),
        ?_assert(is_binary(emqx_variform_bif:hash(sha, 'atom'))),
        ?_assert(is_integer(emqx_variform_bif:hash_to_range(sha, 1, 10))),
        ?_assert(is_integer(emqx_variform_bif:map_to_range(sha, 1, 10))),
        ?_assertEqual(<<"atom ">>, emqx_variform_bif:pad('atom', 5)),
        ?_assertEqual(<<"atom  ">>, emqx_variform_bif:pad('atom', 6))
    ].
