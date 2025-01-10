%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([do_setup_ssl_files/1]).

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
    lists:flatten([
        {"undefined_is_undefined", fun() ->
            ?assertEqual(
                {ok, undefined},
                emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("dir", undefined)
            )
        end},
        {"no_op_if_disabled", fun() ->
            Disabled = #{<<"enable">> => false, foo => bar},
            ?assertEqual(
                {ok, Disabled},
                emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("dir", Disabled)
            )
        end},
        {"mandatory_keys", fun() ->
            ?assertMatch(
                {error, #{missing_options := [<<"keyfile">>]}},
                emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("dir", #{}, #{
                    required_keys => [[<<"keyfile">>]]
                })
            ),
            ?assertMatch(
                {error, #{missing_options := [<<"keyfile">>]}},
                emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("dir", #{}, #{
                    required_keys => [[keyfile]]
                })
            )
        end},
        {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun(Context) ->
            #{cert := Cert, cacert := Cacert} = Context,
            {"invalid_file_path", fun() ->
                ?assertMatch(
                    {error, #{
                        pem_check := not_pem,
                        file_path := not_file_path,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        keyfile => <<"abc", $\n, "123">>,
                        certfile => Cert,
                        cacertfile => Cacert
                    })
                )
            end}
        end},
        {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun(Context) ->
            #{cert := Cert, cacert := Cacert} = Context,
            {"enoent_key_file", fun() ->
                NonExistingFile = filename:join(
                    "/tmp", integer_to_list(erlang:system_time(microsecond))
                ),
                ?assertMatch(
                    {error, #{pem_check := enoent, file_path := _}},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"keyfile">> => NonExistingFile,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => Cacert
                    })
                )
            end}
        end},
        {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun(Context) ->
            #{mk_kit := MkKit} = Context,
            #{key := Key, cert := Cert} =
                MkKit(#{name => "emptycacert", password => undefined}),
            {"empty_cacertfile", fun() ->
                ?assertMatch(
                    {ok, _},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"keyfile">> => Key,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => <<"">>
                    })
                )
            end}
        end},
        {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun(Context) ->
            #{key := Key, cert := Cert, cacert := Cacert, password := Password} = Context,
            {"bad_pem_string", fun() ->
                %% empty string
                ?assertMatch(
                    {error, #{
                        reason := pem_file_path_or_string_is_required,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"keyfile">> => <<>>,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => Cacert
                    })
                ),
                %% not valid unicode
                ?assertMatch(
                    {error, #{
                        reason := invalid_file_path_or_pem_string, which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"keyfile">> => <<255, 255>>,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => Cacert
                    })
                ),
                ?assertMatch(
                    {error, #{
                        reason := invalid_file_path_or_pem_string,
                        which_option := <<"ocsp.issuer_pem">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"password">> => Password,
                        <<"keyfile">> => Key,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => Cacert,
                        <<"ocsp">> => #{<<"issuer_pem">> => <<255, 255>>}
                    })
                ),
                %% not printable
                ?assertMatch(
                    {error, #{reason := invalid_file_path_or_pem_string}},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("/tmp", #{
                        <<"password">> => Password,
                        <<"keyfile">> => <<33, 22>>,
                        <<"certfile">> => Cert,
                        <<"cacertfile">> => Cacert
                    })
                ),
                TmpFile = filename:join("/tmp", integer_to_list(erlang:system_time(microsecond))),
                try
                    ok = file:write_file(TmpFile, <<"not a valid pem">>),
                    ?assertMatch(
                        {error, #{pem_check := not_pem}},
                        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                            "/tmp",
                            #{
                                <<"password">> => Password,
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
        end},
        [
            {setup, setup_ssl_files(#{key_type => KeyType}), fun cleanup_ssl_files/1,
                fun do_test_file_validations/1}
         || KeyType <- [ec, rsa]
        ]
    ]).

do_test_file_validations(Context) ->
    #{mk_kit := MkKit, password := Password} = Context,
    [
        {"corrupt certfile",
            ?_test(begin
                #{cert := Cert, certfile_path := CertfilePath} = MkKit(#{name => "corruptcert"}),
                MangledCert = mangle(Cert),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_certfile,
                        which_option := <<"certfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => MangledCert}
                    )
                ),
                %% Using filepath
                ok = file:write_file(CertfilePath, MangledCert),
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_certfile,
                        which_option := <<"certfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => CertfilePath}
                    )
                )
            end)},
        {"corrupt certfile (more than one certificate inside file)",
            ?_test(begin
                #{cert := Cert, certfile_path := CertfilePath} = MkKit(#{name => "corruptcert"}),
                MangledCert0 = mangle(Cert),
                MangledCert = iolist_to_binary([MangledCert0, "\n", MangledCert0]),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_certfile,
                        which_option := <<"certfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => MangledCert}
                    )
                ),
                %% Using filepath
                ok = file:write_file(CertfilePath, MangledCert),
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_certfile,
                        which_option := <<"certfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => CertfilePath}
                    )
                )
            end)},
        {"valid certfile (more than one certificate inside file)",
            ?_test(begin
                #{cert := Cert0, certfile_path := CertfilePath} = MkKit(#{name => "multicert"}),
                Cert = iolist_to_binary([Cert0, "\n", Cert0]),
                %% Using PEM contents directly
                ?assertMatch(
                    {ok, #{<<"certfile">> := _}},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => Cert}
                    )
                ),
                %% Using filepath
                ok = file:write_file(CertfilePath, Cert),
                ?assertMatch(
                    {ok, #{<<"certfile">> := _}},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"certfile">> => CertfilePath}
                    )
                )
            end)},
        {"corrupt keyfile (with correct password)",
            ?_test(begin
                #{key := Key, keyfile_path := KeyfilePath} = MkKit(#{name => "corruptkey"}),
                MangledKey = mangle(Key),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => MangledKey,
                            <<"password">> => Password
                        }
                    )
                ),
                %% Using filepath
                ok = file:write_file(KeyfilePath, MangledKey),
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => KeyfilePath,
                            <<"password">> => Password
                        }
                    )
                )
            end)},
        {"corrupt keyfile (with incorrect password)",
            ?_test(begin
                #{key := Key, keyfile_path := KeyfilePath} = MkKit(#{name => "corruptkey"}),
                WrongPassword = <<"wrongpass">>,
                MangledKey = mangle(Key),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => MangledKey,
                            <<"password">> => WrongPassword
                        }
                    )
                ),
                %% Using filepath
                ok = file:write_file(KeyfilePath, MangledKey),
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => KeyfilePath,
                            <<"password">> => WrongPassword
                        }
                    )
                )
            end)},
        {"valid encrypted keyfile with incorrect password",
            ?_test(begin
                #{key := Key, keyfile_path := KeyfilePath} = MkKit(#{name => "corruptkey"}),
                WrongPassword = <<"wrongpass">>,
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => Key,
                            <<"password">> => WrongPassword
                        }
                    )
                ),
                %% Using filepath
                ?assertMatch(
                    {error, #{
                        reason := bad_password_or_invalid_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{
                            <<"keyfile">> => KeyfilePath,
                            <<"password">> => WrongPassword
                        }
                    )
                )
            end)},
        {"valid encrypted keyfile missing password",
            ?_test(begin
                #{key := Key, keyfile_path := KeyfilePath} = MkKit(#{name => "corruptkey"}),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := encryped_keyfile_missing_password,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"keyfile">> => Key}
                    )
                ),
                %% Using filepath
                ?assertMatch(
                    {error, #{
                        reason := encryped_keyfile_missing_password,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"keyfile">> => KeyfilePath}
                    )
                )
            end)},
        {"corrupt unencrypted keyfile",
            ?_test(begin
                #{key := Key, keyfile_path := KeyfilePath} =
                    MkKit(#{name => "corruptplainkey", password => undefined}),
                MangledKey = mangle(Key, 0, 10),
                %% Using PEM contents directly
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"keyfile">> => MangledKey}
                    )
                ),
                %% Using filepath
                ok = file:write_file(KeyfilePath, MangledKey),
                ?assertMatch(
                    {error, #{
                        reason := failed_to_parse_keyfile,
                        which_option := <<"keyfile">>
                    }},
                    emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
                        "/tmp",
                        #{<<"keyfile">> => KeyfilePath}
                    )
                )
            end)}
    ].

ssl_file_replace_test() ->
    {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun do_test_ssl_file_replace/1}.
do_test_ssl_file_replace(Context) ->
    #{
        key := Key1,
        cert := Cert1,
        cacert := Cacert1,
        mk_kit := MkKit
    } = Context,
    #{
        key := Key2,
        cert := Cert2,
        cacert := Cacert2
    } = MkKit(#{name => "client2"}),
    SSL0 = #{
        <<"keyfile">> => Key1,
        <<"certfile">> => Cert1,
        <<"cacertfile">> => Cacert1,
        <<"ocsp">> => #{<<"issuer_pem">> => Key1}
    },
    SSL1 = #{
        <<"keyfile">> => Key2,
        <<"certfile">> => Cert2,
        <<"cacertfile">> => Cacert2,
        <<"ocsp">> => #{<<"issuer_pem">> => Key2}
    },
    Dir = filename:join(["/tmp", "ssl-test-dir2"]),
    {ok, SSL2} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(Dir, SSL0),
    {ok, SSL3} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(Dir, SSL1),
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
    {setup, setup_ssl_files(), fun cleanup_ssl_files/1, fun do_test_ssl_file_deterministic_names/1}.
do_test_ssl_file_deterministic_names(Context) ->
    #{
        key := Key,
        cert := Cert
    } = Context,
    SSL0 = #{
        <<"keyfile">> => Key,
        <<"certfile">> => Cert
    },
    Dir0 = filename:join(["/tmp", ?FUNCTION_NAME, "ssl0"]),
    Dir1 = filename:join(["/tmp", ?FUNCTION_NAME, "ssl1"]),
    {ok, SSLFiles0} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(Dir0, SSL0),
    ?assertEqual(
        {ok, SSLFiles0},
        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(Dir0, SSL0)
    ),
    ?assertNotEqual(
        {ok, SSLFiles0},
        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(Dir1, SSL0)
    ),
    _ = file:del_dir_r(filename:join(["/tmp", ?FUNCTION_NAME])).

ssl_file_name_hash_test() ->
    ?assertMatch(
        {ok, #{
            <<"cacertfile">> := <<"data/certs/authz/http/cacert-C62234D748AB82B0">>,
            <<"certfile">> := <<"data/certs/authz/http/cert-0D6E53DBDEF594A4">>,
            <<"enable">> := true,
            <<"keyfile">> := <<"data/certs/authz/http/key-D5BB7F027841FA62">>,
            <<"verify">> := <<"verify_peer">>
        }},
        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
            <<"authz/http">>,
            #{
                <<"cacertfile">> => test_name_hash_cacert(),
                <<"certfile">> => test_name_hash_cert(),
                <<"keyfile">> => test_name_hash_key(),
                <<"enable">> => true,
                <<"verify">> => <<"verify_peer">>
            }
        )
    ),
    ok.

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

password_file_test() ->
    T = integer_to_list(erlang:system_time(microsecond)),
    TmpFile = filename:join("/tmp", "secret-" ++ T ++ ".txt"),
    ok = file:write_file(TmpFile, T),
    ConfigValue = emqx_schema_secret:convert_secret("file://" ++ TmpFile, #{}),
    Options = #{enable => true, password => ConfigValue},
    ClientOpts = emqx_tls_lib:to_client_opts(tls, Options),
    ServerOpts = emqx_tls_lib:to_server_opts(tls, Options),
    {password, PasswordResolved} = lists:keyfind(password, 1, ClientOpts),
    ?assertEqual({password, PasswordResolved}, lists:keyfind(password, 1, ServerOpts)),
    ?assertEqual(T, PasswordResolved),
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

test_name_hash_cacert() ->
    <<
        "-----BEGIN CERTIFICATE-----\n"
        "MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV\n"
        "BAYTAkNOMREwDwYDVQQIDAhoYW5nemhvdTEMMAoGA1UECgwDRU1RMQ8wDQYDVQQD\n"
        "DAZSb290Q0EwHhcNMjAwNTA4MDgwNjUyWhcNMzAwNTA2MDgwNjUyWjA/MQswCQYD\n"
        "VQQGEwJDTjERMA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UE\n"
        "AwwGUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzcgVLex1\n"
        "EZ9ON64EX8v+wcSjzOZpiEOsAOuSXOEN3wb8FKUxCdsGrsJYB7a5VM/Jot25Mod2\n"
        "juS3OBMg6r85k2TWjdxUoUs+HiUB/pP/ARaaW6VntpAEokpij/przWMPgJnBF3Ur\n"
        "MjtbLayH9hGmpQrI5c2vmHQ2reRZnSFbY+2b8SXZ+3lZZgz9+BaQYWdQWfaUWEHZ\n"
        "uDaNiViVO0OT8DRjCuiDp3yYDj3iLWbTA/gDL6Tf5XuHuEwcOQUrd+h0hyIphO8D\n"
        "tsrsHZ14j4AWYLk1CPA6pq1HIUvEl2rANx2lVUNv+nt64K/Mr3RnVQd9s8bK+TXQ\n"
        "KGHd2Lv/PALYuwIDAQABo1AwTjAdBgNVHQ4EFgQUGBmW+iDzxctWAWxmhgdlE8Pj\n"
        "EbQwHwYDVR0jBBgwFoAUGBmW+iDzxctWAWxmhgdlE8PjEbQwDAYDVR0TBAUwAwEB\n"
        "/zANBgkqhkiG9w0BAQsFAAOCAQEAGbhRUjpIred4cFAFJ7bbYD9hKu/yzWPWkMRa\n"
        "ErlCKHmuYsYk+5d16JQhJaFy6MGXfLgo3KV2itl0d+OWNH0U9ULXcglTxy6+njo5\n"
        "CFqdUBPwN1jxhzo9yteDMKF4+AHIxbvCAJa17qcwUKR5MKNvv09C6pvQDJLzid7y\n"
        "E2dkgSuggik3oa0427KvctFf8uhOV94RvEDyqvT5+pgNYZ2Yfga9pD/jjpoHEUlo\n"
        "88IGU8/wJCx3Ds2yc8+oBg/ynxG8f/HmCC1ET6EHHoe2jlo8FpU/SgGtghS1YL30\n"
        "IWxNsPrUP+XsZpBJy/mvOhE5QXo6Y35zDqqj8tI7AGmAWu22jg==\n"
        "-----END CERTIFICATE-----\n"
    >>.

test_name_hash_cert() ->
    <<
        "-----BEGIN CERTIFICATE-----\n"
        "MIIDEzCCAfugAwIBAgIBAjANBgkqhkiG9w0BAQsFADA/MQswCQYDVQQGEwJDTjER\n"
        "MA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UEAwwGUm9vdENB\n"
        "MB4XDTIwMDUwODA4MDcwNVoXDTMwMDUwNjA4MDcwNVowPzELMAkGA1UEBhMCQ04x\n"
        "ETAPBgNVBAgMCGhhbmd6aG91MQwwCgYDVQQKDANFTVExDzANBgNVBAMMBlNlcnZl\n"
        "cjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALNeWT3pE+QFfiRJzKmn\n"
        "AMUrWo3K2j/Tm3+Xnl6WLz67/0rcYrJbbKvS3uyRP/stXyXEKw9CepyQ1ViBVFkW\n"
        "Aoy8qQEOWFDsZc/5UzhXUnb6LXr3qTkFEjNmhj+7uzv/lbBxlUG1NlYzSeOB6/RT\n"
        "8zH/lhOeKhLnWYPXdXKsa1FL6ij4X8DeDO1kY7fvAGmBn/THh1uTpDizM4YmeI+7\n"
        "4dmayA5xXvARte5h4Vu5SIze7iC057N+vymToMk2Jgk+ZZFpyXrnq+yo6RaD3ANc\n"
        "lrc4FbeUQZ5a5s5Sxgs9a0Y3WMG+7c5VnVXcbjBRz/aq2NtOnQQjikKKQA8GF080\n"
        "BQkCAwEAAaMaMBgwCQYDVR0TBAIwADALBgNVHQ8EBAMCBeAwDQYJKoZIhvcNAQEL\n"
        "BQADggEBAJefnMZpaRDHQSNUIEL3iwGXE9c6PmIsQVE2ustr+CakBp3TZ4l0enLt\n"
        "iGMfEVFju69cO4oyokWv+hl5eCMkHBf14Kv51vj448jowYnF1zmzn7SEzm5Uzlsa\n"
        "sqjtAprnLyof69WtLU1j5rYWBuFX86yOTwRAFNjm9fvhAcrEONBsQtqipBWkMROp\n"
        "iUYMkRqbKcQMdwxov+lHBYKq9zbWRoqLROAn54SRqgQk6c15JdEfgOOjShbsOkIH\n"
        "UhqcwRkQic7n1zwHVGVDgNIZVgmJ2IdIWBlPEC7oLrRrBD/X1iEEXtKab6p5o22n\n"
        "KB5mN+iQaE+Oe2cpGKZJiJRdM+IqDDQ=\n"
        "-----END CERTIFICATE-----\n"
    >>.

test_name_hash_key() ->
    <<
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEAs15ZPekT5AV+JEnMqacAxStajcraP9Obf5eeXpYvPrv/Stxi\n"
        "sltsq9Le7JE/+y1fJcQrD0J6nJDVWIFUWRYCjLypAQ5YUOxlz/lTOFdSdvotevep\n"
        "OQUSM2aGP7u7O/+VsHGVQbU2VjNJ44Hr9FPzMf+WE54qEudZg9d1cqxrUUvqKPhf\n"
        "wN4M7WRjt+8AaYGf9MeHW5OkOLMzhiZ4j7vh2ZrIDnFe8BG17mHhW7lIjN7uILTn\n"
        "s36/KZOgyTYmCT5lkWnJeuer7KjpFoPcA1yWtzgVt5RBnlrmzlLGCz1rRjdYwb7t\n"
        "zlWdVdxuMFHP9qrY206dBCOKQopADwYXTzQFCQIDAQABAoIBAQCuvCbr7Pd3lvI/\n"
        "n7VFQG+7pHRe1VKwAxDkx2t8cYos7y/QWcm8Ptwqtw58HzPZGWYrgGMCRpzzkRSF\n"
        "V9g3wP1S5Scu5C6dBu5YIGc157tqNGXB+SpdZddJQ4Nc6yGHXYERllT04ffBGc3N\n"
        "WG/oYS/1cSteiSIrsDy/91FvGRCi7FPxH3wIgHssY/tw69s1Cfvaq5lr2NTFzxIG\n"
        "xCvpJKEdSfVfS9I7LYiymVjst3IOR/w76/ZFY9cRa8ZtmQSWWsm0TUpRC1jdcbkm\n"
        "ZoJptYWlP+gSwx/fpMYftrkJFGOJhHJHQhwxT5X/ajAISeqjjwkWSEJLwnHQd11C\n"
        "Zy2+29lBAoGBANlEAIK4VxCqyPXNKfoOOi5dS64NfvyH4A1v2+KaHWc7lqaqPN49\n"
        "ezfN2n3X+KWx4cviDD914Yc2JQ1vVJjSaHci7yivocDo2OfZDmjBqzaMp/y+rX1R\n"
        "/f3MmiTqMa468rjaxI9RRZu7vDgpTR+za1+OBCgMzjvAng8dJuN/5gjlAoGBANNY\n"
        "uYPKtearBmkqdrSV7eTUe49Nhr0XotLaVBH37TCW0Xv9wjO2xmbm5Ga/DCtPIsBb\n"
        "yPeYwX9FjoasuadUD7hRvbFu6dBa0HGLmkXRJZTcD7MEX2Lhu4BuC72yDLLFd0r+\n"
        "Ep9WP7F5iJyagYqIZtz+4uf7gBvUDdmvXz3sGr1VAoGAdXTD6eeKeiI6PlhKBztF\n"
        "zOb3EQOO0SsLv3fnodu7ZaHbUgLaoTMPuB17r2jgrYM7FKQCBxTNdfGZmmfDjlLB\n"
        "0xZ5wL8ibU30ZXL8zTlWPElST9sto4B+FYVVF/vcG9sWeUUb2ncPcJ/Po3UAktDG\n"
        "jYQTTyuNGtSJHpad/YOZctkCgYBtWRaC7bq3of0rJGFOhdQT9SwItN/lrfj8hyHA\n"
        "OjpqTV4NfPmhsAtu6j96OZaeQc+FHvgXwt06cE6Rt4RG4uNPRluTFgO7XYFDfitP\n"
        "vCppnoIw6S5BBvHwPP+uIhUX2bsi/dm8vu8tb+gSvo4PkwtFhEr6I9HglBKmcmog\n"
        "q6waEQKBgHyecFBeM6Ls11Cd64vborwJPAuxIW7HBAFj/BS99oeG4TjBx4Sz2dFd\n"
        "rzUibJt4ndnHIvCN8JQkjNG14i9hJln+H3mRss8fbZ9vQdqG+2vOWADYSzzsNI55\n"
        "RFY7JjluKcVkp/zCDeUxTU3O6sS+v6/3VE11Cob6OYQx3lN5wrZ3\n"
        "-----END RSA PRIVATE KEY-----\n"
    >>.

setup_ssl_files() ->
    DefaultOpts = #{key_type => ec},
    setup_ssl_files(DefaultOpts).

setup_ssl_files(Opts) ->
    fun() -> do_setup_ssl_files(Opts) end.

do_setup_ssl_files(InOpts) ->
    DefaultKeyType = maps:get(key_type, InOpts),
    BaseTmpDir = maps:get(base_tmp_dir, InOpts, "/tmp"),
    Dir = mktemp_dir(BaseTmpDir),
    DefaultPassword = maps:get(password, InOpts, <<"foobar">>),
    emqx_test_tls_certs_helper:gen_ca(Dir, "root", InOpts),
    MkKit = fun(Opts) ->
        #{name := Name} = Opts,
        Password = maps:get(password, Opts, DefaultPassword),
        KeyType = maps:get(key_type, Opts, DefaultKeyType),
        IntermediateName = "intermediate-" ++ Name,
        GenOpts = #{key_type => KeyType, password => Password},
        %% No password for intermediate key cert
        emqx_test_tls_certs_helper:gen_host_cert(IntermediateName, "root", Dir, GenOpts#{
            password => undefined
        }),
        emqx_test_tls_certs_helper:gen_host_cert(Name, IntermediateName, Dir, GenOpts),
        KeyfilePath = filename:join(Dir, Name ++ ".key"),
        CertfilePath = filename:join(Dir, Name ++ ".pem"),
        CacertfilePath = filename:join(Dir, IntermediateName ++ ".pem"),
        {ok, Key} = file:read_file(KeyfilePath),
        {ok, Cert} = file:read_file(CertfilePath),
        {ok, Cacert} = file:read_file(CacertfilePath),
        #{
            key => Key,
            keyfile_path => KeyfilePath,
            cert => Cert,
            certfile_path => CertfilePath,
            cacert => Cacert,
            cacertfile_path => CacertfilePath
        }
    end,
    #{
        key := Key,
        keyfile_path := KeyfilePath,
        cert := Cert,
        certfile_path := CertfilePath,
        cacert := Cacert,
        cacertfile_path := CacertfilePath
    } = MkKit(#{name => "client"}),
    #{
        cacertfile_path => CacertfilePath,
        certfile_path => CertfilePath,
        keyfile_path => KeyfilePath,
        key => Key,
        cert => Cert,
        cacert => Cacert,
        password => DefaultPassword,
        mk_kit => MkKit,
        temp_dir => Dir
    }.

cleanup_ssl_files(#{temp_dir := Dir}) ->
    file:del_dir_r(Dir).

mktemp_dir(BaseDir) ->
    Dir = os:cmd("mktemp -dp " ++ BaseDir ++ " emqx_tls_lib_tests.XXXXXXXXXXX"),
    string:trim(Dir).

mangle(PEM) ->
    mangle(PEM, 10, 5).

mangle(PEM, Pos, Len) ->
    [{Type, DER, CryptoInfo}] = public_key:pem_decode(PEM),
    CorruptDER = iolist_to_binary([
        binary:part(DER, 0, Pos),
        binary:copy(<<255>>, Len),
        binary:part(DER, Pos + Len, byte_size(DER) - Pos - Len)
    ]),
    public_key:pem_encode([{Type, CorruptDER, CryptoInfo}]).
