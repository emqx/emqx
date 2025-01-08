%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    _ = application:stop(emqx_connector).

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_and(_Config) ->
    ?assertEqual('and'([equalityMatch("a", "1")]), parse("(&(a=1))")),
    ?assertEqual(
        'and'([equalityMatch("a", "1"), (equalityMatch("b", "2"))]),
        parse("(&(a=1)(b=2))")
    ),
    ?assertMatch({error, _}, scan_and_parse("(&)")).

t_or(_Config) ->
    ?assertEqual('or'([equalityMatch("a", "1")]), parse("(|(a=1))")),
    ?assertEqual(
        'or'([equalityMatch("a", "1"), (equalityMatch("b", "2"))]),
        parse("(|(a=1)(b=2))")
    ),
    ?assertMatch({error, _}, scan_and_parse("(|)")).

t_not(_Config) ->
    ?assertEqual('not'(equalityMatch("a", "1")), parse("(!(a=1))")),
    ?assertMatch({error, _}, scan_and_parse("(!)")),
    ?assertMatch({error, _}, scan_and_parse("(!(a=1)(b=1))")).

t_equalityMatch(_Config) ->
    ?assertEqual(equalityMatch("attr", "value"), parse("(attr=value)")),
    ?assertEqual(equalityMatch("attr", "value"), parse("(attr = value)")),
    ?assertMatch({error, _}, scan_and_parse("(attr=)")),
    ?assertMatch({error, _}, scan_and_parse("(=)")),
    ?assertMatch({error, _}, scan_and_parse("(=value)")).

t_substrings_initial(_Config) ->
    ?assertEqual(substrings("attr", [{initial, "initial"}]), parse("(attr=initial*)")),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}]),
        parse("(attr=initial*a*)")
    ),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}, {any, "b"}]),
        parse("(attr=initial*a*b*)")
    ).

t_substrings_final(_Config) ->
    ?assertEqual(substrings("attr", [{final, "final"}]), parse("(attr=*final)")),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {final, "final"}]),
        parse("(attr=*a*final)")
    ),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {any, "b"}, {final, "final"}]),
        parse("(attr=*a*b*final)")
    ).

t_substrings_initial_final(_Config) ->
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {final, "final"}]),
        parse("(attr=initial*final)")
    ),
    ?assertEqual(
        substrings("attr", [{initial, "initial"}, {any, "a"}, {final, "final"}]),
        parse("(attr=initial*a*final)")
    ),
    ?assertEqual(
        substrings(
            "attr",
            [{initial, "initial"}, {any, "a"}, {any, "b"}, {final, "final"}]
        ),
        parse("(attr=initial*a*b*final)")
    ).

t_substrings_only_any(_Config) ->
    ?assertEqual(present("attr"), parse("(attr=*)")),
    ?assertEqual(substrings("attr", [{any, "a"}]), parse("(attr=*a*)")),
    ?assertEqual(
        substrings("attr", [{any, "a"}, {any, "b"}]),
        parse("(attr=*a*b*)")
    ).

t_greaterOrEqual(_Config) ->
    ?assertEqual(greaterOrEqual("attr", "value"), parse("(attr>=value)")),
    ?assertEqual(greaterOrEqual("attr", "value"), parse("(attr >= value  )")),
    ?assertMatch({error, _}, scan_and_parse("(attr>=)")),
    ?assertMatch({error, _}, scan_and_parse("(>=)")),
    ?assertMatch({error, _}, scan_and_parse("(>=value)")).

t_lessOrEqual(_Config) ->
    ?assertEqual(lessOrEqual("attr", "value"), parse("(attr<=value)")),
    ?assertEqual(lessOrEqual("attr", "value"), parse("( attr <= value  )")),
    ?assertMatch({error, _}, scan_and_parse("(attr<=)")),
    ?assertMatch({error, _}, scan_and_parse("(<=)")),
    ?assertMatch({error, _}, scan_and_parse("(<=value)")).

t_present(_Config) ->
    ?assertEqual(present("attr"), parse("(attr=*)")),
    ?assertEqual(present("attr"), parse("( attr = *  )")).

t_approxMatch(_Config) ->
    ?assertEqual(approxMatch("attr", "value"), parse("(attr~=value)")),
    ?assertEqual(approxMatch("attr", "value"), parse("( attr ~= value  )")),
    ?assertMatch({error, _}, scan_and_parse("(attr~=)")),
    ?assertMatch({error, _}, scan_and_parse("(~=)")),
    ?assertMatch({error, _}, scan_and_parse("(~=value)")).

t_extensibleMatch_dn(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {dnAttributes, true}]), parse("(attr:dn:=value)")
    ),
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {dnAttributes, true}]),
        parse("(  attr:dn  :=  value  )")
    ).

t_extensibleMatch_rule(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {matchingRule, "objectClass"}]),
        parse("(attr:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch("value", [{type, "attr"}, {matchingRule, "objectClass"}]),
        parse("(   attr:objectClass  :=  value )")
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
        parse("(attr:dn:objectClass:=value)")
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
        parse("(  attr:dn:objectClass  :=value)")
    ).

t_extensibleMatch_no_dn_rule(_Config) ->
    ?assertEqual(extensibleMatch("value", [{type, "attr"}]), parse("(attr:=value)")),
    ?assertEqual(extensibleMatch("value", [{type, "attr"}]), parse("(  attr  :=  value  )")).

t_extensibleMatch_no_type_dn(_Config) ->
    ?assertEqual(
        extensibleMatch("value", [{matchingRule, "objectClass"}]),
        parse("(:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch("value", [{matchingRule, "objectClass"}]),
        parse("(   :objectClass  :=  value )")
    ).

t_extensibleMatch_no_type_no_dn(_Config) ->
    ?assertEqual(
        extensibleMatch(
            "value",
            [{dnAttributes, true}, {matchingRule, "objectClass"}]
        ),
        parse("(:dn:objectClass:=value)")
    ),
    ?assertEqual(
        extensibleMatch(
            "value",
            [{dnAttributes, true}, {matchingRule, "objectClass"}]
        ),
        parse("(  :dn:objectClass  :=value)")
    ).

t_extensibleMatch_error(_Config) ->
    ?assertMatch({error, _}, scan_and_parse("(:dn:=value)")),
    ?assertMatch({error, _}, scan_and_parse("(::=value)")),
    ?assertMatch({error, _}, scan_and_parse("(:=)")),
    ?assertMatch({error, _}, scan_and_parse("(attr:=)")).

t_error(_Config) ->
    ?assertMatch({error, _}, scan_and_parse("(attr=value")),
    ?assertMatch({error, _}, scan_and_parse("attr=value")),
    ?assertMatch({error, _}, scan_and_parse("(a=b)(c=d)")).

t_escape(_Config) ->
    ?assertEqual(
        'and'([equalityMatch("a", "(value)")]),
        parse("(&(a=\\(value\\)))")
    ),
    ?assertEqual(
        'or'([equalityMatch("a", "name (1)")]),
        parse("(|(a=name \\(1\\)))")
    ),
    ?assertEqual(
        'or'([equalityMatch("a", "name (1) *")]),
        parse("(|(a=name\\ \\(1\\) \\*))")
    ),
    ?assertEqual(
        'and'([equalityMatch("a", "\\value\\")]),
        parse("(&(a=\\\\value\\\\))")
    ).

t_value_eql_dn(_Config) ->
    ?assertEqual('and'([equalityMatch("a", "dn")]), parse("(&(a=dn))")).

t_member_of(_Config) ->
    ?assertEqual(
        'and'([
            equalityMatch("a", "b"), equalityMatch("memberOf", "CN=GroupName,OU=emqx,DC=WL,DC=com")
        ]),
        parse("(&(a=b)(memberOf=CN=GroupName,OU=emqx,DC=WL,DC=com))")
    ).

t_extensible_member_of(_Config) ->
    ?assertEqual(
        'and'([
            equalityMatch("a", "b"),
            extensibleMatch("CN=GroupName,OU=emqx,DC=WL,DC=com", [
                {type, "memberOf"}, {matchingRule, "1.2.840.113556.1.4.1941"}
            ])
        ]),
        parse("(&(a=b)(memberOf:1.2.840.113556.1.4.1941:=CN=GroupName,OU=emqx,DC=WL,DC=com))")
    ).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------
parse(Str) ->
    {ok, Res} = scan_and_parse(Str),
    Res.

scan_and_parse(Str) ->
    emqx_ldap_filter_parser:scan_and_parse(Str).
