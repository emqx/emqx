%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_filter_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(eldap, [
    'and'/1,
    'or'/1,
    'not'/1,
    equalityMatch/2,
    substrings/2,
    present/1,
    greaterOrEqual/2,
    lessOrEqual/2,
    approxMatch/2,
    extensibleMatch/2
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_and(_Config) ->
    ?assertEqual('and'([equalityMatch("a", "1")]), to_eldap_filter("(&(a=1))")),
    ?assertEqual(
        'and'([equalityMatch("a", "1"), (equalityMatch("b", "2"))]),
        to_eldap_filter("(&(a=1)(b=2))")
    ),
    ?assertMatch({error, _}, to_eldap_filter("(&)")).

t_or(_Config) ->
    ?assertEqual('or'([equalityMatch("a", "1")]), to_eldap_filter("(|(a=1))")),
    ?assertEqual(
        'or'([equalityMatch("a", "1"), (equalityMatch("b", "2"))]),
        to_eldap_filter("(|(a=1)(b=2))")
    ),
    ?assertMatch({error, _}, to_eldap_filter("(|)")).

t_not(_Config) ->
    ?assertEqual('not'(equalityMatch("a", "1")), to_eldap_filter("(!(a=1))")),
    ?assertMatch({error, _}, to_eldap_filter("(!)")),
    ?assertMatch({error, _}, to_eldap_filter("(!(a=1)(b=1))")).

t_equalityMatch(_Config) ->
    ?assertEqual(equalityMatch("attr", "value"), to_eldap_filter("(attr=value)")),
    ?assertEqual(equalityMatch("attr", " value"), to_eldap_filter("(attr = value)")),
    ?assertEqual(equalityMatch("attr", ""), to_eldap_filter("(attr=)")),
    ?assertMatch({error, _}, to_eldap_filter("(=)")),
    ?assertMatch({error, _}, to_eldap_filter("(=value)")).

t_substrings_initial(_Config) ->
    ?assertEqual(substrings("attr", [{initial, "="}]), to_eldap_filter("(attr==*)")),
    ?assertEqual(substrings("attr", [{initial, "initial"}]), to_eldap_filter("(attr=initial*)")),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}]),
        to_eldap_filter("(attr=initial*a*)")
    ),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}, {any, "b"}]),
        to_eldap_filter("(attr=initial*a*b*)")
    ).

t_substrings_final(_Config) ->
    ?assertEqual(substrings("attr", [{final, "final"}]), to_eldap_filter("(attr=*final)")),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {final, "final"}]),
        to_eldap_filter("(attr=*a*final)")
    ),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {any, "b"}, {final, "final"}]),
        to_eldap_filter("(attr=*a*b*final)")
    ).

t_substrings_initial_final(_Config) ->
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {final, "final"}]),
        to_eldap_filter("(attr=initial*final)")
    ),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}, {final, "final"}]),
        to_eldap_filter("(attr=initial*a*final)")
    ),
    ?assertEqual(
        substrings(
            "attr",
            [{initial, "initial"}, {any, "a"}, {any, "b"}, {final, "final"}]
        ),
        to_eldap_filter("(attr=initial*a*b*final)")
    ).

t_substrings_only_any(_Config) ->
    ?assertEqual(substrings("attr", [{any, " "}]), to_eldap_filter("(attr=* *)")),
    ?assertEqual(present("attr"), to_eldap_filter("(attr=*)")),
    ?assertEqual(substrings("attr", [{any, "a"}]), to_eldap_filter("(attr=*a*)")),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {any, "b"}]),
        to_eldap_filter("(attr=*a*b*)")
    ),
    ?assertMatch({error, _}, to_eldap_filter("(attr=**)")),
    ?assertMatch({error, _}, to_eldap_filter("(attr= ** )")).

t_greaterOrEqual(_Config) ->
    ?assertEqual(greaterOrEqual("attr", "value"), to_eldap_filter("(attr>=value)")),
    ?assertEqual(greaterOrEqual("attr", " value  "), to_eldap_filter("(attr >= value  )")),
    ?assertEqual(greaterOrEqual("attr", ""), to_eldap_filter("(attr>=)")),
    ?assertMatch({error, _}, to_eldap_filter("(>=)")),
    ?assertMatch({error, _}, to_eldap_filter("(>=value)")).

t_lessOrEqual(_Config) ->
    ?assertEqual(lessOrEqual("attr", "value"), to_eldap_filter("(attr<=value)")),
    ?assertEqual(lessOrEqual("attr", " value  "), to_eldap_filter("( attr <= value  )")),
    ?assertEqual(lessOrEqual("attr", ""), to_eldap_filter("(attr<=)")),
    ?assertMatch({error, _}, to_eldap_filter("(<=)")),
    ?assertMatch({error, _}, to_eldap_filter("(<=value)")).

t_present(_Config) ->
    ?assertEqual(present("attr"), to_eldap_filter("(attr=*)")),
    ?assertEqual(present("attr"), to_eldap_filter("( attr =*)")).

t_approxMatch(_Config) ->
    ?assertEqual(approxMatch("attr", "value"), to_eldap_filter("(attr~=value)")),
    ?assertEqual(approxMatch("attr", " value  "), to_eldap_filter("( attr ~= value  )")),
    ?assertEqual(approxMatch("attr", ""), to_eldap_filter("(attr~=)")),
    ?assertMatch({error, _}, to_eldap_filter("(~=)")),
    ?assertMatch({error, _}, to_eldap_filter("(~=value)")).

t_extensibleMatch_dn(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {dnAttributes, true}]),
        to_eldap_filter("(attr:dn:=value)")
    ),
    ?assertEqual(
        extensibleMatch("  value  ", [{type, "attr"}, {dnAttributes, true}]),
        to_eldap_filter("(  attr:dn  :=  value  )")
    ).

t_extensibleMatch_rule(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {matchingRule, "objectClass"}]),
        to_eldap_filter("(attr:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch("  value ", [{type, "attr"}, {matchingRule, "objectClass"}]),
        to_eldap_filter("(   attr:objectClass  :=  value )")
    ).

t_extensibleMatch_dn_rule(_Config) ->
    ?assertEqual(
        extensibleMatch(
            "value",
            [
                {type, "attr"},
                {dnAttributes, true},
                {matchingRule, "objectClass"}
            ]
        ),
        to_eldap_filter("(attr:dn:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch(
            "value",
            [
                {type, "attr"},
                {dnAttributes, true},
                {matchingRule, "objectClass"}
            ]
        ),
        to_eldap_filter("(  attr:dn:objectClass  :=value)")
    ).

t_extensibleMatch_no_dn_rule(_Config) ->
    ?assertEqual(extensibleMatch("value", [{type, "attr"}]), to_eldap_filter("(attr:=value)")),
    ?assertEqual(
        extensibleMatch("  value  ", [{type, "attr"}]), to_eldap_filter("(  attr  :=  value  )")
    ).

t_extensibleMatch_no_type_dn(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{matchingRule, "objectClass"}]),
        to_eldap_filter("(:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch("  value ", [{matchingRule, "objectClass"}]),
        to_eldap_filter("(   :objectClass  :=  value )")
    ).

t_extensibleMatch_no_type_no_dn(_Config) ->
    ?assertEqual(
        extensibleMatch(
            "value",
            [{dnAttributes, true}, {matchingRule, "objectClass"}]
        ),
        to_eldap_filter("(:dn:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch(
            "value",
            [{dnAttributes, true}, {matchingRule, "objectClass"}]
        ),
        to_eldap_filter("(  :dn:objectClass  :=value)")
    ),
    ?assertEqual(extensibleMatch("", [{type, "attr"}]), to_eldap_filter("(attr:=)")).

t_extensibleMatch_error(_Config) ->
    ?assertMatch({error, _}, to_eldap_filter("(:dn:=value)")),
    ?assertMatch({error, _}, to_eldap_filter("(::=value)")),
    ?assertMatch({error, _}, to_eldap_filter("(:=)")).

t_error(_Config) ->
    ?assertMatch({error, _}, to_eldap_filter("(attr=value")),
    ?assertMatch({error, _}, to_eldap_filter("attr=value")),
    ?assertMatch({error, _}, to_eldap_filter("(a=b)(c=d)")).

t_escape(_Config) ->
    ?assertEqual(
        'and'([equalityMatch("a", "(value)")]),
        to_eldap_filter("(&(a=\\(value\\)))")
    ),
    ?assertEqual(
        'or'([equalityMatch("a", "name (1)")]),
        to_eldap_filter("(|(a=name \\(1\\)))")
    ),
    ?assertEqual(
        'or'([equalityMatch("a", "name (1) *")]),
        to_eldap_filter("(|(a=name\\ \\(1\\) \\*))")
    ),
    ?assertEqual(
        'and'([equalityMatch("a", "\\value\\")]),
        to_eldap_filter("(&(a=\\\\value\\\\))")
    ),
    ?assertEqual(
        substrings("a", [{initial, "*"}, {any, "*"}, {final, "*"}]),
        to_eldap_filter("(a=\\**\\**\\*)")
    ),
    ?assertEqual(
        substrings("a", [{initial, " "}, {any, "*"}, {final, " "}]),
        to_eldap_filter("(a= *\\** )")
    ).

t_value_eql_dn(_Config) ->
    ?assertEqual('and'([equalityMatch("a", "dn")]), to_eldap_filter("(&(a=dn))")).

t_member_of(_Config) ->
    ?assertEqual(
        'and'([
            equalityMatch("a", "b"), equalityMatch("memberOf", "CN=GroupName,OU=emqx,DC=WL,DC=com")
        ]),
        to_eldap_filter("(&(a=b)(memberOf=CN=GroupName,OU=emqx,DC=WL,DC=com))")
    ).

t_extensible_member_of(_Config) ->
    ?assertEqual(
        'and'([
            equalityMatch("a", "b"),
            extensibleMatch("CN=GroupName,OU=emqx,DC=WL,DC=com", [
                {type, "memberOf"}, {matchingRule, "1.2.840.113556.1.4.1941"}
            ])
        ]),
        to_eldap_filter(
            "(&(a=b)(memberOf:1.2.840.113556.1.4.1941:=CN=GroupName,OU=emqx,DC=WL,DC=com))"
        )
    ).

t_mapfold_values(_Config) ->
    {ok, Filter0} = emqx_ldap_filter:parse("(&(a=b)(c=d*e*f)(memberOf:dn:=g))"),
    {Filter1, Acc} = emqx_ldap_filter:mapfold_values(
        fun(V, Acc0) -> {string:to_upper(V), Acc0 ++ V} end, "", Filter0
    ),
    ?assertEqual(
        emqx_ldap_filter:to_eldap(Filter1),
        to_eldap_filter("(&(a=B)(c=D*E*F)(memberOf:dn:=G))")
    ),
    ?assertEqual("bdefg", Acc).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

to_eldap_filter(Str) ->
    maybe
        {ok, Filter} ?= emqx_ldap_filter:parse(Str),
        emqx_ldap_filter:to_eldap(Filter)
    end.

scan_and_parse(Str) ->
    ct:print("Tokens: ~p", [scan(Str)]),
    emqx_ldap_filter_parser:scan_and_parse(Str).

scan(Str) ->
    emqx_ldap_filter_lexer:string(Str).
