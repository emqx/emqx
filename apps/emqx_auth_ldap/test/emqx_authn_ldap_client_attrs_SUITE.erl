%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authn_ldap_client_attrs_SUITE).

-moduledoc """
Tests for the `client_attrs` mapping of the LDAP authenticator, run once for the `hash` method
and once for the `bind` method.

The fixture user `mqttgroupuser` has these `memberOf` values, in this order (see
`apps/emqx_ldap/test/data/emqx.groups.ldif`):

```
cn=Employees-T1,ou=Resource Groups,ou=Groups,dc=emqx,dc=io
cn=GROUP-X-ADMIN,ou=Resource Groups,ou=Groups,dc=emqx,dc=io
cn=GROUP-Y,ou=Resource Groups,ou=Groups,dc=emqx,dc=io
cn=GROUP-Z,ou=Resource Groups,ou=Groups,dc=emqx,dc=io
cn=Group-Mixed-Case,ou=Resource Groups,ou=Groups,dc=emqx,dc=io
cn=GROUP-P,ou=Tier-1,ou=Groups,dc=emqx,dc=io
```

The user is not a member of `GROUP-X`, whose name is a prefix of `GROUP-X-ADMIN`.
OpenLDAP returns the attribute types in lower case (`cn=`), so the patterns here use `^cn=`.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_DEFAULT_PORT, 389).

-define(PATH, [authentication]).
-define(AUTHN_ID, <<"password_based:ldap">>).

-define(USER, <<"mqttgroupuser">>).
-define(DN(CN), <<"cn=", CN, ",ou=Resource Groups,ou=Groups,dc=emqx,dc=io">>).

all() ->
    [{group, hash}, {group, bind}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{hash, [], Tests}, {bind, [], Tests}].

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, emqx_authn_test_lib:emqx_appspec()},
            emqx_auth,
            emqx_auth_ldap
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(Method, Config) ->
    [{method, Method} | Config].

end_per_group(_Method, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    Config.

end_per_testcase(_, _Config) ->
    emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

-doc "Checks that one entry matching one `memberOf` value sets its attribute to the extracted CN.".
t_single_entry(Config) ->
    ok = create(Config, #{<<"client_attrs">> => [entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>])]}),
    ?assertEqual({ok, #{<<"grp_y">> => <<"GROUP-Y">>}}, client_attrs(?USER)).

-doc "Checks that `extract = value` sets the whole selected value.".
t_extract_value(Config) ->
    Entry = (entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>]))#{<<"extract">> => <<"value">>},
    ok = create(Config, #{<<"client_attrs">> => [Entry]}),
    ?assertEqual({ok, #{<<"grp_y">> => ?DN("GROUP-Y")}}, client_attrs(?USER)).

-doc """
Checks that `extract = literal` sets the configured string when a pattern matches, and
nothing when no pattern matches.
""".
t_extract_literal(Config) ->
    Entry = (entry(<<"grp">>, [<<"^cn=GROUP-Y,">>]))#{
        <<"extract">> => <<"literal">>, <<"literal">> => <<"grp123">>
    },
    ok = create(Config, #{<<"client_attrs">> => [Entry]}),
    ?assertEqual({ok, #{<<"grp">> => <<"grp123">>}}, client_attrs(?USER)),
    ok = update(Config, #{<<"client_attrs">> => [Entry#{<<"select">> => [<<"^cn=GROUP-X,">>]}]}),
    ?assertEqual({ok, #{}}, client_attrs(?USER)).

-doc """
Checks that `extract = literal` and `literal` must be configured together, and that the
literal must not be empty.
""".
t_literal_config(Config) ->
    Base = entry(<<"grp">>, [<<"^cn=GROUP-Y,">>]),
    lists:foreach(
        fun({Entry, Reason}) ->
            Result = create_result(Config, #{<<"client_attrs">> => [Entry]}),
            ?assertMatch({error, _}, Result, Entry),
            ErrorText = iolist_to_binary(io_lib:format("~0p", [Result])),
            ?assertNotEqual(nomatch, binary:match(ErrorText, Reason), ErrorText)
        end,
        [
            {Base#{<<"extract">> => <<"literal">>}, <<"missing_literal">>},
            {Base#{<<"extract">> => <<"literal">>, <<"literal">> => <<>>}, <<"missing_literal">>},
            {Base#{<<"literal">> => <<"grp123">>}, <<"unexpected_literal">>}
        ]
    ).

-doc """
Checks that several entries match at once and each sets its own attribute, and that an entry
whose group the user is not in leaves its attribute unset.
""".
t_several_entries(Config) ->
    ok = create(Config, #{
        <<"client_attrs">> => [
            entry(<<"grp_x">>, [<<"^cn=GROUP-X,">>]),
            entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>]),
            entry(<<"grp_z">>, [<<"^cn=GROUP-Z,">>]),
            entry(<<"grp_p">>, [<<"^cn=GROUP-P,">>])
        ]
    }),
    ?assertEqual(
        {ok, #{
            <<"grp_y">> => <<"GROUP-Y">>,
            <<"grp_z">> => <<"GROUP-Z">>,
            <<"grp_p">> => <<"GROUP-P">>
        }},
        client_attrs(?USER)
    ).

-doc """
Checks that when no entry matches, authentication succeeds without client attributes by
default, and the client is denied when `require_client_attrs` is true.
""".
t_no_entry_matched(Config) ->
    Entries = [
        entry(<<"grp_x">>, [<<"^cn=GROUP-X,">>]),
        entry(<<"grp_w">>, [<<"^cn=GROUP-W,">>])
    ],
    ok = create(Config, #{<<"client_attrs">> => Entries}),
    ?assertEqual({ok, #{}}, client_attrs(?USER)),
    ok = update(Config, #{<<"client_attrs">> => Entries, <<"require_client_attrs">> => true}),
    ?assertEqual({error, not_authorized}, authenticate(?USER)).

-doc """
Checks that with `require_client_attrs` true, one matching entry out of several is enough to
allow the client.
""".
t_require_one_of_several(Config) ->
    ok = create(Config, #{
        <<"client_attrs">> => [
            entry(<<"grp_x">>, [<<"^cn=GROUP-X,">>]),
            entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>]),
            entry(<<"grp_w">>, [<<"^cn=GROUP-W,">>])
        ],
        <<"require_client_attrs">> => true
    }),
    ?assertEqual({ok, #{<<"grp_y">> => <<"GROUP-Y">>}}, client_attrs(?USER)).

-doc """
Pins the documented anchoring hazard. The user is in `GROUP-X-ADMIN` but not in `GROUP-X`.
The anchored pattern `^cn=GROUP-X,` does not match, and the unanchored pattern `cn=GROUP-X`
does match `cn=GROUP-X-ADMIN,...`.
""".
t_prefix_overlap(Config) ->
    ok = create(Config, #{<<"client_attrs">> => [entry(<<"grp_x">>, [<<"^cn=GROUP-X,">>])]}),
    ?assertEqual({ok, #{}}, client_attrs(?USER)),
    ok = update(Config, #{<<"client_attrs">> => [entry(<<"grp_x">>, [<<"cn=GROUP-X">>])]}),
    ?assertEqual({ok, #{<<"grp_x">> => <<"GROUP-X-ADMIN">>}}, client_attrs(?USER)).

-doc """
Checks that matching is case-sensitive, that `(?i)` makes it case-insensitive, and that the
extracted CN keeps the casing returned by the directory.
""".
t_case(Config) ->
    ok = create(Config, #{
        <<"client_attrs">> => [entry(<<"grp_m">>, [<<"^cn=GROUP-MIXED-CASE,">>])]
    }),
    ?assertEqual({ok, #{}}, client_attrs(?USER)),
    ok = update(Config, #{
        <<"client_attrs">> => [entry(<<"grp_m">>, [<<"(?i)^cn=GROUP-MIXED-CASE,">>])]
    }),
    ?assertEqual({ok, #{<<"grp_m">> => <<"Group-Mixed-Case">>}}, client_attrs(?USER)).

-doc """
Checks the selection order: the pattern order decides first, and within one pattern the first
value in directory order wins.
""".
t_pattern_order(Config) ->
    ok = create(Config, #{
        <<"client_attrs">> => [
            entry(<<"by_pattern">>, [<<"^cn=GROUP-W,">>, <<"^cn=GROUP-Z,">>, <<"^cn=GROUP-Y,">>]),
            entry(<<"by_value">>, [<<"^cn=GROUP-[YZ],">>])
        ]
    }),
    ?assertEqual(
        {ok, #{<<"by_pattern">> => <<"GROUP-Z">>, <<"by_value">> => <<"GROUP-Y">>}},
        client_attrs(?USER)
    ).

-doc """
Checks that a malformed regular expression fails the config update with an error that names
the pattern, and that clients keep authenticating with the previous config.
""".
t_invalid_regex(Config) ->
    Bad = <<"^cn=(GROUP-Y,">>,
    ?assertMatch(
        {error, _},
        create_result(Config, #{<<"client_attrs">> => [entry(<<"grp_y">>, [Bad])]})
    ),
    ok = create(Config, #{<<"client_attrs">> => [entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>])]}),
    Result = update_result(Config, #{
        <<"client_attrs">> => [entry(<<"grp_y">>, [<<"^cn=GROUP-Z,">>, Bad])]
    }),
    ?assertMatch({error, _}, Result),
    ErrorText = iolist_to_binary(io_lib:format("~0p", [Result])),
    ?assertNotEqual(nomatch, binary:match(ErrorText, Bad), ErrorText),
    ?assertNotEqual(nomatch, binary:match(ErrorText, <<"client_attrs">>), ErrorText),
    ?assertEqual({ok, #{<<"grp_y">> => <<"GROUP-Y">>}}, client_attrs(?USER)).

-doc "Checks that an empty `select` list fails the config update.".
t_empty_select(Config) ->
    ?assertMatch(
        {error, _},
        create_result(Config, #{<<"client_attrs">> => [entry(<<"grp_y">>, [])]})
    ).

-doc "Checks that `require_client_attrs` without any `client_attrs` entry fails the config update.".
t_require_without_entries(Config) ->
    ?assertMatch(
        {error, _},
        create_result(Config, #{<<"require_client_attrs">> => true})
    ).

-doc "Checks that an invalid client attribute name fails the config update.".
t_invalid_attr_name(Config) ->
    ?assertMatch(
        {error, _},
        create_result(Config, #{
            <<"client_attrs">> => [entry(<<"grp y">>, [<<"^cn=GROUP-Y,">>])]
        })
    ).

-doc "Checks that a directory attribute name that is not an LDAP attribute name fails the config update.".
t_invalid_attribute(Config) ->
    lists:foreach(
        fun(Name) ->
            Entry = (entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>]))#{<<"attribute">> => Name},
            Result = create_result(Config, #{<<"client_attrs">> => [Entry]}),
            ?assertMatch({error, _}, Result, Name),
            ErrorText = iolist_to_binary(io_lib:format("~0p", [Result])),
            ?assertNotEqual(
                nomatch, binary:match(ErrorText, <<"Invalid LDAP attribute name">>), ErrorText
            )
        end,
        [<<"member of">>, <<"gr", 16#C3, 16#BC, "ppe">>, <<"memberOf\n">>]
    ).

-doc """
Checks that an attribute absent from the user's entry sets no client attribute, and denies
the client when `require_client_attrs` is true.
""".
t_absent_attribute(Config) ->
    Entry = (entry(<<"dept">>, [<<".">>]))#{<<"attribute">> => <<"departmentNumber">>},
    ok = create(Config, #{<<"client_attrs">> => [Entry]}),
    ?assertEqual({ok, #{}}, client_attrs(?USER)),
    ok = update(Config, #{<<"client_attrs">> => [Entry], <<"require_client_attrs">> => true}),
    ?assertEqual({error, not_authorized}, authenticate(?USER)).

-doc "Checks that the directory attribute name is matched case-insensitively.".
t_attribute_name_case(Config) ->
    Entry = (entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>]))#{<<"attribute">> => <<"MEMBEROF">>},
    ok = create(Config, #{<<"client_attrs">> => [Entry]}),
    ?assertEqual({ok, #{<<"grp_y">> => <<"GROUP-Y">>}}, client_attrs(?USER)).

-doc "Checks that `extract = cn` sets no attribute when the selected value is not a DN.".
t_extract_cn_from_non_dn(Config) ->
    Entry = (entry(<<"uid">>, [<<"^mqttgroupuser$">>]))#{<<"attribute">> => <<"uid">>},
    ok = create(Config, #{<<"client_attrs">> => [Entry]}),
    ?assertEqual({ok, #{}}, client_attrs(?USER)),
    ok = update(Config, #{<<"client_attrs">> => [Entry#{<<"extract">> => <<"value">>}]}),
    ?assertEqual({ok, #{<<"uid">> => ?USER}}, client_attrs(?USER)).

-doc "Checks that a wrong password is still rejected when a `client_attrs` entry would match.".
t_wrong_password(Config) ->
    ok = create(Config, #{<<"client_attrs">> => [entry(<<"grp_y">>, [<<"^cn=GROUP-Y,">>])]}),
    ?assertMatch({error, _}, authenticate(?USER, <<"wrongpassword">>)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

entry(SetAs, Select) ->
    #{
        <<"attribute">> => <<"memberOf">>,
        <<"set_as_attr">> => SetAs,
        <<"select">> => Select,
        <<"extract">> => <<"cn">>
    }.

create(Config, Params) ->
    {ok, _} = create_result(Config, Params),
    ok.

create_result(Config, Params) ->
    emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, maps:merge(raw_config(Config), Params)}
    ).

update(Config, Params) ->
    {ok, _} = update_result(Config, Params),
    ok.

update_result(Config, Params) ->
    emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, ?AUTHN_ID, maps:merge(raw_config(Config), Params)}
    ).

authenticate(Username) ->
    authenticate(Username, Username).

authenticate(Username, Password) ->
    emqx_access_control:authenticate(#{
        username => Username,
        password => Password,
        listener => 'tcp:default',
        protocol => mqtt
    }).

client_attrs(Username) ->
    case authenticate(Username) of
        {ok, Result} -> {ok, maps:get(client_attrs, Result, #{})};
        Error -> Error
    end.

raw_config(Config) ->
    Common = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"server">> => ldap_server(),
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 2
    },
    case ?config(method, Config) of
        hash ->
            Common#{
                <<"base_dn">> => <<"uid=${username},ou=groupmember,dc=emqx,dc=io">>,
                <<"method">> => #{<<"type">> => <<"hash">>}
            };
        bind ->
            Common#{
                <<"base_dn">> => <<"ou=groupmember,dc=emqx,dc=io">>,
                <<"filter">> => <<"(uid=${username})">>,
                <<"method">> => #{
                    <<"type">> => <<"bind">>,
                    <<"bind_password">> => <<"${password}">>
                }
            }
    end.

ldap_server() ->
    iolist_to_binary(io_lib:format("~s:~B", [?LDAP_HOST, ?LDAP_DEFAULT_PORT])).
