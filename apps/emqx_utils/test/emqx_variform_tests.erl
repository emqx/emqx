%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_variform_tests).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SYNTAX_ERROR, {error, "syntax error before:" ++ _}).

render_test_() ->
    [
        {"direct var reference", fun() -> ?assertEqual({ok, <<"1">>}, render("a", #{a => 1})) end},
        {"direct var reference missing", fun() ->
            ?assertMatch({error, #{reason := var_unbound}}, render("a", #{}))
        end},
        {"direct var reference undefined", fun() ->
            ?assertEqual({ok, <<"">>}, render("a", #{a => undefined}))
        end},
        {"var reference undefined", fun() ->
            ?assertEqual(
                {ok, <<"/c1">>}, render("concat([a, '/', c])", #{a => undefined, c => <<"c1">>})
            )
        end},
        {"direct var reference null", fun() ->
            ?assertEqual({ok, <<"">>}, render("a", #{a => null}))
        end},
        {"direct var reference emptry str", fun() ->
            ?assertEqual({ok, <<"">>}, render("a", #{a => <<>>}))
        end},
        {"concat strings", fun() ->
            ?assertEqual({ok, <<"a,b">>}, render("concat(['a',',','b'])", #{}))
        end},
        {"concat empty string", fun() ->
            ?assertEqual({ok, <<"">>}, render("concat([''])", #{}))
        end},
        {"identifier with hyphen", fun() ->
            ?assertEqual(
                {ok, <<"10">>},
                render(
                    "pub_props.Message-Expiry-Interval",
                    #{pub_props => #{'Message-Expiry-Interval' => 10}}
                )
            )
        end},
        {"tokens 1st", fun() ->
            ?assertEqual({ok, <<"a">>}, render("nth(1,tokens(var, ','))", #{var => <<"a,b">>}))
        end},
        {"unknown var return error", fun() ->
            ?assertMatch({error, #{reason := var_unbound}}, render("var", #{}))
        end},
        {"out of range nth index", fun() ->
            ?assertEqual({ok, <<>>}, render("nth(2, tokens(var, ','))", #{var => <<"a">>}))
        end},
        {"string for nth index", fun() ->
            ?assertEqual({ok, <<"a">>}, render("nth('1', tokens(var, ','))", #{var => <<"a">>}))
        end},
        {"not a index number for nth", fun() ->
            ?assertMatch(
                {error, #{reason := invalid_argument, func := nth, index := <<"notnum">>}},
                render("nth('notnum', tokens(var, ','))", #{var => <<"a">>})
            )
        end},
        {"substr", fun() ->
            ?assertMatch(
                {ok, <<"b">>},
                render("substr(var,1)", #{var => <<"ab">>})
            )
        end},
        {"result in integer", fun() ->
            ?assertMatch(
                {ok, <<"2">>},
                render("strlen(var)", #{var => <<"ab">>})
            )
        end},
        {"result in float", fun() ->
            ?assertMatch(
                {ok, <<"2.2">>},
                render("var", #{var => 2.2})
            )
        end},
        {"concat a number", fun() ->
            ?assertMatch(
                {ok, <<"2.2">>},
                render("concat(strlen(var),'.2')", #{var => <<"xy">>})
            )
        end},
        {"var is an array", fun() ->
            ?assertMatch(
                {ok, <<"y">>},
                render("nth(2,var)", #{var => [<<"x">>, <<"y">>]})
            )
        end}
    ].

unknown_func_test_() ->
    [
        {"unknown function", fun() ->
            ?assertMatch(
                {error, #{reason := unknown_variform_function}},
                render("nonexistingatom__(a)", #{})
            )
        end},
        {"unknown module", fun() ->
            ?assertMatch(
                {error, #{reason := unknown_variform_module}},
                render("nonexistingatom__.nonexistingatom__(a)", #{})
            )
        end},
        {"unknown function in a known module", fun() ->
            ?assertMatch(
                {error, #{reason := unknown_variform_function}},
                render("emqx_variform_bif.nonexistingatom__(a)", #{})
            )
        end},
        {"invalid func reference", fun() ->
            ?assertMatch(
                {error, #{reason := invalid_function_reference, function := "a.b.c"}},
                render("a.b.c(var)", #{})
            )
        end}
    ].

concat(L) -> iolist_to_binary(L).

inject_allowed_module_test() ->
    try
        emqx_variform:inject_allowed_module(?MODULE),
        ?assertEqual({ok, <<"ab">>}, render(atom_to_list(?MODULE) ++ ".concat(['a','b'])", #{})),
        ?assertMatch(
            {error, #{
                reason := unknown_variform_function,
                module := ?MODULE,
                function := concat,
                arity := 2
            }},
            render(atom_to_list(?MODULE) ++ ".concat('a','b')", #{})
        ),
        ?assertMatch(
            {error, #{reason := unallowed_variform_module, module := emqx}},
            render("emqx.concat('a','b')", #{})
        )
    after
        emqx_variform:erase_allowed_module(?MODULE)
    end.

coalesce_test_() ->
    [
        {"first", fun() ->
            ?assertEqual({ok, <<"a">>}, render("coalesce(['a','b'])", #{}))
        end},
        {"second", fun() ->
            ?assertEqual({ok, <<"b">>}, render("coalesce(['', 'b'])", #{}))
        end},
        {"first var", fun() ->
            ?assertEqual({ok, <<"a">>}, render("coalesce([a,b])", #{a => <<"a">>, b => <<"b">>}))
        end},
        {"second var", fun() ->
            ?assertEqual({ok, <<"b">>}, render("coalesce([a,b])", #{b => <<"b">>}))
        end},
        {"empty", fun() -> ?assertEqual({ok, <<>>}, render("coalesce([a,b])", #{})) end},
        {"arg from other func", fun() ->
            ?assertEqual({ok, <<"b">>}, render("coalesce(tokens(a,','))", #{a => <<",,b,c">>}))
        end},
        {"arg from other func, but no result", fun() ->
            ?assertEqual({ok, <<"">>}, render("coalesce(tokens(a,','))", #{a => <<",,,">>}))
        end},
        {"var unbound", fun() -> ?assertEqual({ok, <<>>}, render("coalesce(a)", #{})) end},
        {"var unbound in call", fun() ->
            ?assertEqual({ok, <<>>}, render("coalesce(concat(a))", #{}))
        end},
        {"var unbound in calls", fun() ->
            ?assertEqual({ok, <<"c">>}, render("coalesce([any_to_str(a),any_to_str(b),'c'])", #{}))
        end},
        {"coalesce n-args", fun() ->
            ?assertEqual(
                {ok, <<"2">>}, render("coalesce(a,b)", #{a => <<"">>, b => 2})
            )
        end},
        {"coalesce 1-arg", fun() ->
            ?assertMatch(
                {error, #{reason := coalesce_badarg}}, render("coalesce(any_to_str(a))", #{a => 1})
            )
        end}
    ].

boolean_literal_test_() ->
    [
        ?_assertEqual({ok, <<"true">>}, render("true", #{})),
        ?_assertEqual({ok, <<"T">>}, render("iif(true,'T','F')", #{}))
    ].

compare_string_test_() ->
    [
        %% is_nil test
        ?_assertEqual({ok, <<"true">>}, render("is_empty('')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("is_empty(a)", #{<<"a">> => undefined})),
        ?_assertEqual({ok, <<"true">>}, render("is_empty(a)", #{<<"a">> => null})),
        ?_assertEqual({ok, <<"false">>}, render("is_empty('a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("is_empty(a)", #{<<"a">> => "1"})),

        %% Testing str_eq/2
        ?_assertEqual({ok, <<"true">>}, render("str_eq('a', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_eq('a', 'b')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_eq('', '')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_eq('a', '')", #{})),
        ?_assertEqual(
            {ok, <<"true">>}, render("str_eq(a, b)", #{<<"a">> => <<"1">>, <<"b">> => <<"1">>})
        ),

        %% Testing str_neq/2
        ?_assertEqual({ok, <<"false">>}, render("str_neq('a', 'a')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_neq('a', 'b')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_neq('', '')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_neq('a', '')", #{})),
        ?_assertEqual(
            {ok, <<"false">>}, render("str_neq(a, b)", #{<<"a">> => <<"1">>, <<"b">> => <<"1">>})
        ),

        %% Testing str_lt/2
        ?_assertEqual({ok, <<"true">>}, render("str_lt('a', 'b')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_lt('b', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_lt('a', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_lt('', '')", #{})),

        ?_assertEqual({ok, <<"true">>}, render("str_gt('b', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_gt('a', 'b')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_gt('a', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_gt('', '')", #{})),

        ?_assertEqual({ok, <<"true">>}, render("str_lte('a', 'b')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_lte('a', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_lte('b', 'a')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_lte('', '')", #{})),

        ?_assertEqual({ok, <<"true">>}, render("str_gte('b', 'a')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_gte('a', 'a')", #{})),
        ?_assertEqual({ok, <<"false">>}, render("str_gte('a', 'b')", #{})),
        ?_assertEqual({ok, <<"true">>}, render("str_gte('', '')", #{})),

        ?_assertEqual({ok, <<"true">>}, render("str_gt(9, 10)", #{}))
    ].

compare_numbers_test_() ->
    [
        ?_assertEqual({ok, <<"true">>}, render("num_eq(1, 1)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_eq(2, 1)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_eq(a, b)", #{<<"a">> => 1, <<"b">> => 2})),

        ?_assertEqual({ok, <<"false">>}, render("num_neq(1, 1)", #{})),
        ?_assertEqual({ok, <<"true">>}, render("num_neq(2, 1)", #{})),
        ?_assertEqual({ok, <<"true">>}, render("num_neq(a, b)", #{<<"a">> => 1, <<"b">> => 2})),

        ?_assertEqual({ok, <<"true">>}, render("num_lt(1, 2)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_lt(2, 2)", #{})),

        ?_assertEqual({ok, <<"true">>}, render("num_gt(2, 1)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_gt(1, 1)", #{})),

        ?_assertEqual({ok, <<"true">>}, render("num_lte(1, 1)", #{})),
        ?_assertEqual({ok, <<"true">>}, render("num_lte(1, 2)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_lte(2, 1)", #{})),

        ?_assertEqual({ok, <<"true">>}, render("num_gte(2, -1)", #{})),
        ?_assertEqual({ok, <<"true">>}, render("num_gte(2, 2)", #{})),
        ?_assertEqual({ok, <<"false">>}, render("num_gte(-1, 2)", #{}))
    ].

syntax_error_test_() ->
    [
        {"empty expression", fun() -> ?assertMatch(?SYNTAX_ERROR, render("", #{})) end},
        {"const string single quote", fun() -> ?assertMatch(?SYNTAX_ERROR, render("'a'", #{})) end},
        {"const string double quote", fun() ->
            ?assertMatch(?SYNTAX_ERROR, render(<<"\"a\"">>, #{}))
        end}
    ].

maps_test_() ->
    [
        {"arity zero", ?_assertEqual({ok, <<"0">>}, render(<<"maps.size(maps.new())">>, #{}))}
    ].

render(Expression, Bindings) ->
    emqx_variform:render(Expression, Bindings).

hash_pick_test() ->
    lists:foreach(
        fun(_) ->
            {ok, Res} = render("nth(hash_to_range(rand_str(10),1,5),[1,2,3,4,5])", #{}),
            ?assert(Res >= <<"1">> andalso Res =< <<"5">>)
        end,
        lists:seq(1, 100)
    ).

map_to_range_pick_test() ->
    lists:foreach(
        fun(_) ->
            {ok, Res} = render("nth(map_to_range(rand_str(10),1,5),[1,2,3,4,5])", #{}),
            ?assert(Res >= <<"1">> andalso Res =< <<"5">>)
        end,
        lists:seq(1, 100)
    ).

-define(ASSERT_BADARG(FUNC, ARGS),
    ?_assertEqual(
        {error, #{reason => badarg, function => FUNC}},
        render(atom_to_list(FUNC) ++ ARGS, #{})
    )
).

to_range_badarg_test_() ->
    [
        ?ASSERT_BADARG(hash_to_range, "(1,1,2)"),
        ?ASSERT_BADARG(hash_to_range, "('',1,2)"),
        ?ASSERT_BADARG(hash_to_range, "('a','1',2)"),
        ?ASSERT_BADARG(hash_to_range, "('a',2,1)"),
        ?ASSERT_BADARG(map_to_range, "('',1,2)"),
        ?ASSERT_BADARG(map_to_range, "('a','1',2)"),
        ?ASSERT_BADARG(map_to_range, "('a',2,1)")
    ].

iif_test_() ->
    %% if clientid has two words separated by a -, take the suffix, and append with `/#`
    Expr1 = "iif(nth(2,tokens(clientid,'-')),concat([nth(2,tokens(clientid,'-')),'/#']),'')",
    [
        ?_assertEqual({ok, <<"yes-A">>}, render("iif(a,'yes-A','no-A')", #{a => <<"x">>})),
        ?_assertEqual({ok, <<"no-A">>}, render("iif(a,'yes-A','no-A')", #{})),
        ?_assertEqual({ok, <<"2">>}, render("iif(str_eq(a,1),2,3)", #{a => 1})),
        ?_assertEqual({ok, <<"3">>}, render("iif(str_eq(a,1),2,3)", #{a => <<"not-1">>})),
        ?_assertEqual({ok, <<"3">>}, render("iif(str_eq(a,1),2,3)", #{})),
        ?_assertEqual({ok, <<"">>}, render(Expr1, #{clientid => <<"a">>})),
        ?_assertEqual({ok, <<"suffix/#">>}, render(Expr1, #{clientid => <<"a-suffix">>}))
    ].
