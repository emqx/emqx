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

redner_test_() ->
    [
        {"direct var reference", fun() -> ?assertEqual({ok, <<"1">>}, render("a", #{a => 1})) end},
        {"concat strings", fun() ->
            ?assertEqual({ok, <<"a,b">>}, render("concat('a',',','b')", #{}))
        end},
        {"concat empty string", fun() -> ?assertEqual({ok, <<"">>}, render("concat('')", #{})) end},
        {"tokens 1st", fun() ->
            ?assertEqual({ok, <<"a">>}, render("nth(1,tokens(var, ','))", #{var => <<"a,b">>}))
        end},
        {"unknown var as empty str", fun() ->
            ?assertEqual({ok, <<>>}, render("var", #{}))
        end},
        {"out of range nth index", fun() ->
            ?assertEqual({ok, <<>>}, render("nth(2, tokens(var, ','))", #{var => <<"a">>}))
        end},
        {"not a index number for nth", fun() ->
            ?assertMatch(
                {error, #{reason := invalid_argument, func := nth, index := <<"notnum">>}},
                render("nth('notnum', tokens(var, ','))", #{var => <<"a">>})
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
                render("emqx_variform_str.nonexistingatom__(a)", #{})
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
            {error, #{reason := unallowed_veriform_module, module := emqx}},
            render("emqx.concat('a','b')", #{})
        )
    after
        emqx_variform:erase_allowed_module(?MODULE)
    end.

coalesce_test_() ->
    [
        {"coalesce first", fun() ->
            ?assertEqual({ok, <<"a">>}, render("coalesce('a','b')", #{}))
        end},
        {"coalesce second", fun() ->
            ?assertEqual({ok, <<"b">>}, render("coalesce('', 'b')", #{}))
        end},
        {"coalesce first var", fun() ->
            ?assertEqual({ok, <<"a">>}, render("coalesce(a,b)", #{a => <<"a">>, b => <<"b">>}))
        end},
        {"coalesce second var", fun() ->
            ?assertEqual({ok, <<"b">>}, render("coalesce(a,b)", #{b => <<"b">>}))
        end},
        {"coalesce empty", fun() -> ?assertEqual({ok, <<>>}, render("coalesce(a,b)", #{})) end}
    ].

syntax_error_test_() ->
    [
        {"empty expression", fun() -> ?assertMatch(?SYNTAX_ERROR, render("", #{})) end},
        {"const string single quote", fun() -> ?assertMatch(?SYNTAX_ERROR, render("'a'", #{})) end},
        {"const string double quote", fun() ->
            ?assertMatch(?SYNTAX_ERROR, render(<<"\"a\"">>, #{}))
        end},
        {"no arity", fun() -> ?assertMatch(?SYNTAX_ERROR, render("concat()", #{})) end}
    ].

render(Expression, Bindings) ->
    emqx_variform:render(Expression, Bindings).
