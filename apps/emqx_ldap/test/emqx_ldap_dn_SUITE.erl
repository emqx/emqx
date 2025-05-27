%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        {error, {invalid_string_char, _}},
        emqx_ldap_dn:parse("cn=X" ++ [255])
    ),
    ?assertMatch(
        {error, {invalid_string_char, _}},
        emqx_ldap_dn:parse("cn=\\X")
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
