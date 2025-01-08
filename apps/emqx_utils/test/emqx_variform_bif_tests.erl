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
