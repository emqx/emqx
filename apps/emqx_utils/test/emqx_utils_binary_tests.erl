%%--------------------------------------------------------------------
%% Original file taken from https://github.com/arcusfelis/binary2
%% Copyright (c) 2016 Michael Uvarov
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
-module(emqx_utils_binary_tests).

-import(emqx_utils_binary, [
    trim/1,
    ltrim/1,
    rtrim/1,
    trim/2,
    ltrim/2,
    rtrim/2,
    reverse/1,
    inverse/1,
    join/2,
    suffix/2,
    prefix/2,
    duplicate/2,
    union/2,
    intersection/2,
    subtract/2,
    optimize_patterns/1
]).

-include_lib("eunit/include/eunit.hrl").

trim1_test_() ->
    [
        ?_assertEqual(trim(<<>>), <<>>),
        ?_assertEqual(trim(<<0, 0, 0>>), <<>>),
        ?_assertEqual(trim(<<1, 2, 3>>), <<1, 2, 3>>),
        ?_assertEqual(trim(<<0, 1, 2>>), <<1, 2>>),
        ?_assertEqual(trim(<<0, 0, 1, 2>>), <<1, 2>>),
        ?_assertEqual(trim(<<1, 2, 0, 0>>), <<1, 2>>),
        ?_assertEqual(trim(<<0, 1, 2, 0>>), <<1, 2>>),
        ?_assertEqual(trim(<<0, 0, 0, 1, 2, 0, 0, 0>>), <<1, 2>>)
    ].

ltrim1_test_() ->
    [
        ?_assertEqual(ltrim(<<>>), <<>>),
        ?_assertEqual(ltrim(<<0, 0, 0>>), <<>>),
        ?_assertEqual(ltrim(<<1, 2, 3>>), <<1, 2, 3>>),
        ?_assertEqual(ltrim(<<0, 1, 2>>), <<1, 2>>),
        ?_assertEqual(ltrim(<<0, 0, 1, 2>>), <<1, 2>>),
        ?_assertEqual(ltrim(<<1, 2, 0, 0>>), <<1, 2, 0, 0>>),
        ?_assertEqual(ltrim(<<0, 1, 2, 0>>), <<1, 2, 0>>),
        ?_assertEqual(ltrim(<<0, 0, 0, 1, 2, 0, 0, 0>>), <<1, 2, 0, 0, 0>>)
    ].

rtrim1_test_() ->
    [
        ?_assertEqual(rtrim(<<>>), <<>>),
        ?_assertEqual(rtrim(<<1, 2, 3>>), <<1, 2, 3>>),
        ?_assertEqual(rtrim(<<0, 0, 0>>), <<>>),
        ?_assertEqual(rtrim(<<0, 1, 2>>), <<0, 1, 2>>),
        ?_assertEqual(rtrim(<<0, 0, 1, 2>>), <<0, 0, 1, 2>>),
        ?_assertEqual(rtrim(<<1, 2, 0, 0>>), <<1, 2>>),
        ?_assertEqual(rtrim(<<0, 1, 2, 0>>), <<0, 1, 2>>),
        ?_assertEqual(rtrim(<<0, 0, 0, 1, 2, 0, 0, 0>>), <<0, 0, 0, 1, 2>>)
    ].

trim2_test_() ->
    [
        ?_assertEqual(trim(<<5>>, 5), <<>>),
        ?_assertEqual(trim(<<5, 1, 2, 5>>, 5), <<1, 2>>),
        ?_assertEqual(trim(<<5, 5, 5, 1, 2, 0, 0, 0>>, 5), <<1, 2, 0, 0, 0>>)
    ].

ltrim2_test_() ->
    [
        ?_assertEqual(ltrim(<<5>>, 5), <<>>),
        ?_assertEqual(ltrim(<<5, 1, 2, 5>>, 5), <<1, 2, 5>>),
        ?_assertEqual(ltrim(<<5, 5, 5, 1, 2, 0, 0, 0>>, 5), <<1, 2, 0, 0, 0>>)
    ].

rtrim2_test_() ->
    [
        ?_assertEqual(rtrim(<<5>>, 5), <<>>),
        ?_assertEqual(rtrim(<<5, 1, 2, 5>>, 5), <<5, 1, 2>>),
        ?_assertEqual(rtrim(<<5, 5, 5, 1, 2, 0, 0, 0>>, 5), <<5, 5, 5, 1, 2, 0, 0, 0>>)
    ].

mtrim2_test_() ->
    [
        ?_assertEqual(trim(<<5>>, [1, 5]), <<>>),
        ?_assertEqual(trim(<<5, 1, 2, 5>>, [1, 5]), <<2>>),
        ?_assertEqual(trim(<<5, 1, 2, 5>>, [1, 2, 5]), <<>>),
        ?_assertEqual(trim(<<5, 5, 5, 1, 2, 0, 0, 0>>, [1, 5]), <<2, 0, 0, 0>>)
    ].

mltrim2_test_() ->
    [
        ?_assertEqual(ltrim(<<5>>, [1, 5]), <<>>),
        ?_assertEqual(ltrim(<<5, 1, 2, 5>>, [1, 5]), <<2, 5>>),
        ?_assertEqual(ltrim(<<5, 1, 2, 5>>, [2, 5]), <<1, 2, 5>>),
        ?_assertEqual(ltrim(<<5, 5, 5, 1, 2, 0, 0, 0>>, [1, 5]), <<2, 0, 0, 0>>)
    ].

mrtrim2_test_() ->
    [
        ?_assertEqual(rtrim(<<5>>, [1, 5]), <<>>),
        ?_assertEqual(rtrim(<<5, 1, 2, 5>>, [1, 5]), <<5, 1, 2>>),
        ?_assertEqual(rtrim(<<5, 1, 2, 5>>, [2, 5]), <<5, 1>>),
        ?_assertEqual(rtrim(<<5, 5, 5, 1, 2, 0, 0, 0>>, [1, 5]), <<5, 5, 5, 1, 2, 0, 0, 0>>),
        ?_assertEqual(rtrim(<<5, 5, 5, 1, 2, 0, 0, 0>>, [0, 5]), <<5, 5, 5, 1, 2>>)
    ].

reverse_test_() ->
    [?_assertEqual(reverse(<<0, 1, 2>>), <<2, 1, 0>>)].

join_test_() ->
    [
        ?_assertEqual(join([<<1, 2>>, <<3, 4>>, <<5, 6>>], <<0>>), <<1, 2, 0, 3, 4, 0, 5, 6>>),
        ?_assertEqual(
            join([<<"abc">>, <<"def">>, <<"xyz">>], <<"|">>),
            <<"abc|def|xyz">>
        ),
        ?_assertEqual(
            join([<<>>, <<"|">>, <<"x|z">>], <<"|">>),
            <<"|||x|z">>
        ),
        ?_assertEqual(
            join([<<"abc">>, <<"def">>, <<"xyz">>], <<>>),
            <<"abcdefxyz">>
        ),
        ?_assertEqual(join([], <<"|">>), <<>>)
    ].

duplicate_test_() ->
    [
        ?_assertEqual(duplicate(5, <<1, 2>>), <<1, 2, 1, 2, 1, 2, 1, 2, 1, 2>>),
        ?_assertEqual(duplicate(50, <<0>>), <<0:400>>)
    ].

suffix_test_() ->
    [
        ?_assertEqual(suffix(<<1, 2, 3, 4, 5>>, 2), <<4, 5>>),
        ?_assertError(badarg, prefix(<<1, 2, 3, 4, 5>>, 25))
    ].

prefix_test_() ->
    [
        ?_assertEqual(prefix(<<1, 2, 3, 4, 5>>, 2), <<1, 2>>),
        ?_assertError(badarg, prefix(<<1, 2, 3, 4, 5>>, 25))
    ].

union_test_() ->
    [
        ?_assertEqual(
            union(
                <<2#0011011:7>>,
                <<2#1011110:7>>
            ),
            <<2#1011111:7>>
        )
    ].

inverse_test_() ->
    [
        ?_assertEqual(inverse(inverse(<<0, 1, 2>>)), <<0, 1, 2>>),
        ?_assertEqual(inverse(<<0>>), <<255>>),
        ?_assertEqual(inverse(<<2#1:1>>), <<2#0:1>>),
        ?_assertEqual(inverse(<<2#0:1>>), <<2#1:1>>),
        ?_assertEqual(
            inverse(<<2#01:2>>),
            <<2#10:2>>
        ),
        ?_assertEqual(
            inverse(<<2#0011011:7>>),
            <<2#1100100:7>>
        )
    ].

intersection_test_() ->
    [
        ?_assertEqual(
            intersection(
                <<2#0011011>>,
                <<2#1011110>>
            ),
            <<2#0011010>>
        )
    ].

subtract_test_() ->
    [
        ?_assertEqual(
            subtract(
                <<2#0011011>>,
                <<2#1011110>>
            ),
            <<2#0000001>>
        )
    ].

optimize_patterns_test_() ->
    [
        ?_assertEqual(
            [<<"t">>],
            optimize_patterns([<<"t">>, <<"test">>])
        ),
        ?_assertEqual(
            [<<"t">>],
            optimize_patterns([<<"t">>, <<"t">>, <<"test">>])
        ),
        ?_assertEqual(
            [<<"t">>],
            optimize_patterns([<<"test">>, <<"t">>, <<"t">>])
        )
    ].
