%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_dn_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("emqx_ldap.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_parse_dn(_Config) ->
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", "John"}, {"sn", "Doe"}, {"ou", "Users"}, {"dc", "com"}]]}},
        emqx_ldap_dn:parse("cn=John+sn=Doe+ou=Users+dc=com")
    ),
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", "John"}, {"sn", "Doe"}], [{"ou", "Users"}, {"dc", "c m"}]]}},
        emqx_ldap_dn:parse(" cn=John+sn=Doe, ou= Users + dc = c m ")
    ),
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", " John Doe \"123\" " ++ [255]}]]}},
        emqx_ldap_dn:parse(" cn=\\ John\\ Doe\\ \\\"123\\\" \\ff")
    ),
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", "John"}], [{"1.2.3;foo", {hexstring, "11ae"}}]]}},
        emqx_ldap_dn:parse("cn=John,1.2.3;foo=#11ae")
    ),
    ?assertMatch(
        {error, {invalid_attr_value_pair, _}},
        emqx_ldap_dn:parse("cnJohn,1.2.3;foo=#11ae")
    ),
    ?assertMatch(
        {error, empty_value},
        emqx_ldap_dn:parse("cn=,1.2.3;foo=#11ae")
    ),
    ?assertMatch(
        {error, empty_hexstring},
        emqx_ldap_dn:parse("cn=John,1.2.3;foo=#")
    ),
    ?assertMatch(
        {error, invalid_hexstring},
        emqx_ldap_dn:parse("cn=John,1.2.3;foo=#1")
    ),
    ?assertMatch(
        {error, invalid_hexstring},
        emqx_ldap_dn:parse("cn=John,1.2.3;foo=#XY")
    ),
    ?assertMatch(
        {error, invalid_utf8},
        emqx_ldap_dn:parse("cn=X" ++ [255])
    ),
    ?assertMatch(
        {error, {invalid_string_char, _}},
        emqx_ldap_dn:parse("cn=\\X")
    ).

-doc "Checks that a value may contain unescaped multi-byte UTF-8 characters, and only those.".
t_parse_dn_utf8(_Config) ->
    %% "Grün", with the u-umlaut as UTF-8 bytes
    Gruen = "Gr" ++ [16#C3, 16#BC] ++ "n",
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", Gruen}], [{"dc", "x"}]]}},
        emqx_ldap_dn:parse(<<"cn=Gr", 16#C3, 16#BC, "n,dc=x">>)
    ),
    %% a four-byte character at the end of the value
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"cn", "a" ++ [16#F0, 16#9F, 16#98, 16#80]}]]}},
        emqx_ldap_dn:parse(<<"cn=a", 16#F0, 16#9F, 16#98, 16#80>>)
    ),
    %% a truncated sequence
    ?assertMatch(
        {error, invalid_utf8},
        emqx_ldap_dn:parse(<<"cn=Gr", 16#C3, ",dc=x">>)
    ),
    %% a lone continuation byte
    ?assertMatch(
        {error, invalid_utf8},
        emqx_ldap_dn:parse(<<"cn=Gr", 16#BC, "n">>)
    ),
    %% an overlong encoding and a surrogate
    ?assertMatch(
        {error, invalid_utf8},
        emqx_ldap_dn:parse(<<"cn=", 16#C0, 16#80>>)
    ),
    ?assertMatch(
        {error, invalid_utf8},
        emqx_ldap_dn:parse(<<"cn=", 16#ED, 16#A0, 16#80>>)
    ).

-doc """
Checks that trimming a value strips only unescaped ASCII whitespace: a UTF-8 byte that is a
Unicode whitespace code point (16#85, U+0085) and an escaped trailing space are kept.
""".
t_parse_dn_trim(_Config) ->
    %% "Group-ą", with the a-ogonek as UTF-8 bytes C4 85
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"CN", "Group-" ++ [16#C4, 16#85]}], [{"DC", "x"}]]}},
        emqx_ldap_dn:parse(<<"CN=Group-", 16#C4, 16#85, ",DC=x">>)
    ),
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"CN", "Group-A "}], [{"DC", "x"}]]}},
        emqx_ldap_dn:parse(<<"CN=Group-A\\ ,DC=x">>)
    ),
    %% an escaped space followed by an unescaped one
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"CN", "Group-A "}], [{"DC", "x"}]]}},
        emqx_ldap_dn:parse(<<"CN=Group-A\\  ,DC=x">>)
    ),
    %% an escaped backslash followed by an unescaped space
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"CN", "Group-A\\"}], [{"DC", "x"}]]}},
        emqx_ldap_dn:parse(<<"CN=Group-A\\\\ ,DC=x">>)
    ),
    ?assertEqual(
        {ok, #ldap_dn{dn = [[{"CN", " Group-A"}], [{"DC", "x"}]]}},
        emqx_ldap_dn:parse(<<"CN= \\ Group-A\t,DC=x">>)
    ),
    %% to_string/1 escapes the spaces at both ends, and parse/1 reads them back
    DN = #ldap_dn{dn = [[{"cn", " John Doe "}]]},
    ?assertEqual({ok, DN}, emqx_ldap_dn:parse(dn_to_string(DN))).

-doc "Checks which strings are accepted as LDAP attribute descriptions.".
t_is_attribute_description(_Config) ->
    lists:foreach(
        fun(Name) -> ?assert(emqx_ldap_dn:is_attribute_description(Name), Name) end,
        [<<"memberOf">>, "cn", <<"x-Custom-1">>, <<"2.5.4.3">>, <<"userCertificate;binary">>]
    ),
    lists:foreach(
        fun(Name) -> ?assertNot(emqx_ldap_dn:is_attribute_description(Name), Name) end,
        [
            <<>>,
            <<"member of">>,
            <<" memberOf">>,
            <<"memberOf\n">>,
            <<"1member">>,
            <<"member_of">>,
            <<"2.5..3">>,
            <<"memberOf;">>,
            <<"gr", 16#C3, 16#BC, "ppe">>
        ]
    ).

t_to_string(_Config) ->
    ?assertEqual(
        "cn=John+sn=Doe,ou=Users+dc=c m",
        dn_to_string(#ldap_dn{
            dn = [[{"cn", "John"}, {"sn", "Doe"}], [{"ou", "Users"}, {"dc", "c m"}]]
        })
    ),
    ?assertEqual(
        "cn=\\ John Doe \\\"123\\\" \\ff\\ ",
        dn_to_string(#ldap_dn{dn = [[{"cn", " John Doe \"123\" " ++ [255] ++ " "}]]})
    ).

t_mapfold_values(_Config) ->
    {ok, DN} = emqx_ldap_dn:parse(" cn=John+sn=Doe, ou= Users + dc = c m "),
    {ok, DNExpected} = emqx_ldap_dn:parse("cn=JOHN+sn=DOE,ou=USERS+dc=C M"),
    ?assertEqual(
        {DNExpected, 4},
        emqx_ldap_dn:mapfold_values(
            fun(Value, Acc) -> {string:uppercase(Value), Acc + 1} end, 0, DN
        )
    ).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

dn_to_string(DN) ->
    lists:flatten(emqx_ldap_dn:to_string(DN)).
