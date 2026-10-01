%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_client_attrs_tests).

-moduledoc """
Unit tests for `emqx_authn_ldap_client_attrs` on hand-built LDAP entries, for group DNs that
the test directory cannot hold.
""".

-include_lib("eunit/include/eunit.hrl").
-include_lib("eldap/include/eldap.hrl").

%% "Group-ą", with the a-ogonek as UTF-8 bytes C4 85. The byte 16#85 is also the
%% code point U+0085, which Unicode classifies as whitespace.
-define(OGONEK, 16#C4, 16#85).

extract_cn_utf8_test() ->
    Entry = entry([[$C, $N, $=, $G, $r, $o, $u, $p, $-, ?OGONEK | ",OU=Groups,DC=x"]]),
    ?assertEqual(
        {ok, #{client_attrs => #{<<"grp">> => <<"Group-", ?OGONEK>>}}},
        from_entry(Entry, [<<"^CN=Group-", ?OGONEK, ",">>], true)
    ).

extract_cn_escaped_trailing_space_test() ->
    Entry = entry(["CN=Group-A\\ ,OU=Groups,DC=x"]),
    ?assertEqual(
        {ok, #{client_attrs => #{<<"grp">> => <<"Group-A ">>}}},
        from_entry(Entry, [<<"^CN=Group-A\\\\ ,">>], true)
    ).

%% A selected value whose CN cannot be extracted sets no attribute, so
%% require_client_attrs denies the client.
extract_cn_invalid_dn_test() ->
    Entry = entry(["CN=Group-A\\"]),
    ?assertEqual(
        {error, no_client_attrs},
        from_entry(Entry, [<<"^CN=Group-A">>], true)
    ).

entry(MemberOf) ->
    #eldap_entry{
        object_name = "uid=u,ou=users,dc=x",
        attributes = [{"memberOf", MemberOf}]
    }.

from_entry(Entry, Select, Require) ->
    {ok, Compiled} = emqx_authn_ldap_client_attrs:compile([
        #{attribute => <<"memberOf">>, set_as_attr => <<"grp">>, select => Select, extract => cn}
    ]),
    emqx_authn_ldap_client_attrs:from_entry(Entry, #{
        client_attrs => Compiled,
        require_client_attrs => Require
    }).
