%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tls_lib_tests).

-include_lib("eunit/include/eunit.hrl").

%% one of the cipher suite from tlsv1.2 and tlsv1.3 each
-define(TLS_12_CIPHER, "ECDHE-ECDSA-AES256-GCM-SHA384").
-define(TLS_13_CIPHER, "TLS_AES_256_GCM_SHA384").

ensure_tls13_ciphers_added_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3'], [?TLS_12_CIPHER]),
    ?assert(lists:member(?TLS_12_CIPHER, Ciphers)),
    ?assert(lists:member(?TLS_13_CIPHER, Ciphers)).

legacy_cipher_suites_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.2'], [?TLS_12_CIPHER]),
    ?assertEqual([?TLS_12_CIPHER], Ciphers).

use_default_ciphers_test() ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3', 'tlsv1.2'], ""),
    ?assert(lists:member(?TLS_12_CIPHER, Ciphers)),
    ?assert(lists:member(?TLS_13_CIPHER, Ciphers)).

ciphers_format_test_() ->
    String = ?TLS_13_CIPHER ++ "," ++ ?TLS_12_CIPHER,
    Binary = bin(String),
    List = [?TLS_13_CIPHER, ?TLS_12_CIPHER],
    [
        {"string", fun() -> test_cipher_format(String) end},
        {"binary", fun() -> test_cipher_format(Binary) end},
        {"string-list", fun() -> test_cipher_format(List) end}
    ].

test_cipher_format(Input) ->
    Ciphers = emqx_tls_lib:integral_ciphers(['tlsv1.3', 'tlsv1.2'], Input),
    ?assertEqual([?TLS_13_CIPHER, ?TLS_12_CIPHER], Ciphers).

tls_versions_test() ->
    ?assert(lists:member('tlsv1.3', emqx_tls_lib:available_versions(tls))).

tls_version_unknown_test_() ->
    lists:flatmap(
        fun(Type) ->
            [
                ?_assertEqual(
                    emqx_tls_lib:available_versions(Type),
                    emqx_tls_lib:integral_versions(Type, [])
                ),
                ?_assertEqual(
                    emqx_tls_lib:available_versions(Type),
                    emqx_tls_lib:integral_versions(Type, <<>>)
                ),
                ?_assertEqual(
                    emqx_tls_lib:available_versions(Type),
                    %% unknown version dropped
                    emqx_tls_lib:integral_versions(Type, "foo")
                ),
                fun() ->
                    ?assertError(
                        #{reason := no_available_tls_version},
                        emqx_tls_lib:integral_versions(Type, [foo])
                    )
                end
            ]
        end,
        [tls, dtls]
    ).

cipher_suites_no_duplication_test() ->
    AllCiphers = emqx_tls_lib:default_ciphers(),
    ?assertEqual(length(AllCiphers), length(lists:usort(AllCiphers))).

ssl_files_failure_test_() ->
    [
        {"undefined_is_undefined", fun() ->
            ?assertEqual(
                {ok, undefined},
                emqx_tls_lib:ensure_ssl_files("dir", undefined)
            )
        end},
        {"no_op_if_disabled", fun() ->
            Disabled = #{<<"enable">> => false, foo => bar},
            ?assertEqual(
                {ok, Disabled},
                emqx_tls_lib:ensure_ssl_files("dir", Disabled)
            )
        end},
        {"enoent_key_file", fun() ->
            NonExistingFile = filename:join(
                "/tmp", integer_to_list(erlang:system_time(microsecond))
            ),
            ?assertMatch(
                {error, #{file_read := enoent, pem_check := invalid_pem}},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => NonExistingFile,
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => test_key()
                })
            )
        end},
        {"empty_cacertfile", fun() ->
            ?assertMatch(
                {ok, _},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => test_key(),
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => <<"">>
                })
            )
        end},
        {"bad_pem_string", fun() ->
            %% empty string
            ?assertMatch(
                {error, #{
                    reason := pem_file_path_or_string_is_required,
                    which_options := [[<<"keyfile">>]]
                }},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => <<>>,
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => test_key()
                })
            ),
            %% not valid unicode
            ?assertMatch(
                {error, #{
                    reason := invalid_file_path_or_pem_string, which_options := [[<<"keyfile">>]]
                }},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => <<255, 255>>,
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => test_key()
                })
            ),
            ?assertMatch(
                {error, #{
                    reason := invalid_file_path_or_pem_string,
                    which_options := [[<<"ocsp">>, <<"issuer_pem">>]]
                }},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => test_key(),
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => test_key(),
                    <<"ocsp">> => #{<<"issuer_pem">> => <<255, 255>>}
                })
            ),
            %% not printable
            ?assertMatch(
                {error, #{reason := invalid_file_path_or_pem_string}},
                emqx_tls_lib:ensure_ssl_files("/tmp", #{
                    <<"keyfile">> => <<33, 22>>,
                    <<"certfile">> => test_key(),
                    <<"cacertfile">> => test_key()
                })
            ),
            TmpFile = filename:join("/tmp", integer_to_list(erlang:system_time(microsecond))),
            try
                ok = file:write_file(TmpFile, <<"not a valid pem">>),
                ?assertMatch(
                    {error, #{file_read := not_pem}},
                    emqx_tls_lib:ensure_ssl_files(
                        "/tmp",
                        #{
                            <<"cacertfile">> => bin(TmpFile),
                            <<"keyfile">> => bin(TmpFile),
                            <<"certfile">> => bin(TmpFile),
                            <<"ocsp">> => #{<<"issuer_pem">> => bin(TmpFile)}
                        }
                    )
                )
            after
                file:delete(TmpFile)
            end
        end}
    ].

ssl_file_replace_test() ->
    Key1 = test_key(),
    Key2 = test_key2(),
    SSL0 = #{
        <<"keyfile">> => Key1,
        <<"certfile">> => Key1,
        <<"cacertfile">> => Key1,
        <<"ocsp">> => #{<<"issuer_pem">> => Key1}
    },
    SSL1 = #{
        <<"keyfile">> => Key2,
        <<"certfile">> => Key2,
        <<"cacertfile">> => Key2,
        <<"ocsp">> => #{<<"issuer_pem">> => Key2}
    },
    Dir = filename:join(["/tmp", "ssl-test-dir2"]),
    {ok, SSL2} = emqx_tls_lib:ensure_ssl_files(Dir, SSL0),
    {ok, SSL3} = emqx_tls_lib:ensure_ssl_files(Dir, SSL1),
    File1 = maps:get(<<"keyfile">>, SSL2),
    File2 = maps:get(<<"keyfile">>, SSL3),
    IssuerPem1 = emqx_utils_maps:deep_get([<<"ocsp">>, <<"issuer_pem">>], SSL2),
    IssuerPem2 = emqx_utils_maps:deep_get([<<"ocsp">>, <<"issuer_pem">>], SSL3),
    ?assert(filelib:is_regular(File1)),
    ?assert(filelib:is_regular(File2)),
    ?assert(filelib:is_regular(IssuerPem1)),
    ?assert(filelib:is_regular(IssuerPem2)),
    ok.

ssl_file_deterministic_names_test() ->
    SSL0 = #{
        <<"keyfile">> => test_key(),
        <<"certfile">> => test_key()
    },
    Dir0 = filename:join(["/tmp", ?FUNCTION_NAME, "ssl0"]),
    Dir1 = filename:join(["/tmp", ?FUNCTION_NAME, "ssl1"]),
    {ok, SSLFiles0} = emqx_tls_lib:ensure_ssl_files(Dir0, SSL0),
    ?assertEqual(
        {ok, SSLFiles0},
        emqx_tls_lib:ensure_ssl_files(Dir0, SSL0)
    ),
    ?assertNotEqual(
        {ok, SSLFiles0},
        emqx_tls_lib:ensure_ssl_files(Dir1, SSL0)
    ),
    _ = file:del_dir_r(filename:join(["/tmp", ?FUNCTION_NAME])).

to_client_opts_test() ->
    VersionsAll = [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3'],
    Versions13Only = ['tlsv1.3'],
    Options = #{
        enable => true,
        verify => verify_none,
        server_name_indication => "SNI",
        ciphers => "Ciphers",
        depth => "depth",
        password => "password",
        versions => VersionsAll,
        secure_renegotiate => "secure_renegotiate",
        reuse_sessions => "reuse_sessions"
    },
    Expected0 = lists:usort(maps:keys(Options) -- [enable]),
    Expected1 = lists:sort(Expected0 ++ [customize_hostname_check]),
    ?assertEqual(
        Expected0, lists:usort(proplists:get_keys(emqx_tls_lib:to_client_opts(tls, Options)))
    ),
    ?assertEqual(
        Expected1,
        lists:usort(
            proplists:get_keys(emqx_tls_lib:to_client_opts(tls, Options#{verify => verify_peer}))
        )
    ),
    Expected2 =
        lists:usort(
            maps:keys(Options) --
                [enable, reuse_sessions, secure_renegotiate]
        ),
    ?assertEqual(
        Expected2,
        lists:usort(
            proplists:get_keys(
                emqx_tls_lib:to_client_opts(tls, Options#{versions := Versions13Only})
            )
        )
    ),
    Expected3 = lists:usort(maps:keys(Options) -- [enable, depth, password]),
    ?assertEqual(
        Expected3,
        lists:usort(
            proplists:get_keys(
                emqx_tls_lib:to_client_opts(tls, Options#{depth := undefined, password := ""})
            )
        )
    ),
    ok.

to_server_opts_test() ->
    VersionsAll = [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3'],
    Versions13Only = ['tlsv1.3'],
    Options = #{
        verify => "Verify",
        ciphers => "Ciphers",
        versions => VersionsAll,
        user_lookup_fun => "funfunfun",
        client_renegotiation => "client_renegotiation"
    },
    Expected1 = lists:usort(maps:keys(Options)),
    ?assertEqual(
        Expected1, lists:usort(proplists:get_keys(emqx_tls_lib:to_server_opts(tls, Options)))
    ),
    Expected2 = lists:usort(maps:keys(Options) -- [user_lookup_fun, client_renegotiation]),
    ?assertEqual(
        Expected2,
        lists:usort(
            proplists:get_keys(
                emqx_tls_lib:to_server_opts(tls, Options#{versions := Versions13Only})
            )
        )
    ).

bin(X) -> iolist_to_binary(X).

test_key() ->
    <<
        "\n"
        "-----BEGIN EC PRIVATE KEY-----\n"
        "MHQCAQEEICKTbbathzvD8zvgjL7qRHhW4alS0+j0Loo7WeYX9AxaoAcGBSuBBAAK\n"
        "oUQDQgAEJBdF7MIdam5T4YF3JkEyaPKdG64TVWCHwr/plC0QzNVJ67efXwxlVGTo\n"
        "ju0VBj6tOX1y6C0U+85VOM0UU5xqvw==\n"
        "-----END EC PRIVATE KEY-----\n"
    >>.

test_key2() ->
    <<
        "\n"
        "-----BEGIN EC PRIVATE KEY-----\n"
        "MHQCAQEEID9UlIyAlLFw0irkRHX29N+ZGivGtDjlVJvATY3B0TTmoAcGBSuBBAAK\n"
        "oUQDQgAEUwiarudRNAT25X11js8gE9G+q0GdsT53QJQjRtBO+rTwuCW1vhLzN0Ve\n"
        "AbToUD4JmV9m/XwcSVH06ZaWqNuC5w==\n"
        "-----END EC PRIVATE KEY-----\n"
    >>.
