%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_arrow_flight_connector_tests).

-include_lib("eunit/include/eunit.hrl").

ssl_opts_test_() ->
    CacertFile = bin(cacert_file()),
    [
        ?_assertEqual(
            #{},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>, <<"datalayers.example.com">>, #{enable => false}
            )
        ),
        ?_assertEqual(
            #{
                tls_enable => true,
                tls_cacertfile => CacertFile,
                verify => verify_peer,
                tls_server_name => <<"datalayers.example.com">>
            },
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile
                }
            )
        ),
        ?_assertEqual(
            #{
                tls_enable => true,
                tls_cacertfile => CacertFile,
                verify => verify_peer,
                tls_server_name => <<"server.example.com">>
            },
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"127.0.0.1">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    server_name_indication => <<"server.example.com">>
                }
            )
        ),
        ?_test(
            with_system_default_ca(CacertFile, fun() ->
                ?assertEqual(
                    #{
                        tls_enable => true,
                        tls_cacertfile => CacertFile,
                        verify => verify_peer,
                        tls_server_name => <<"datalayers.example.com">>
                    },
                    emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                        <<"test:ssl_opts">>,
                        <<"datalayers.example.com">>,
                        #{
                            enable => true,
                            verify => verify_peer,
                            cacertfile => <<"system_default">>
                        }
                    )
                )
            end)
        ),
        ?_test(
            without_system_default_ca(fun() ->
                ?assertEqual(
                    {error, ssl_cacertfile_is_required},
                    emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                        <<"test:ssl_opts">>,
                        <<"datalayers.example.com">>,
                        #{
                            enable => true,
                            verify => verify_peer,
                            cacertfile => <<"system_default">>
                        }
                    )
                )
            end)
        ),
        ?_assertEqual(
            {error, verify_none_not_supported},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_none,
                    cacertfile => CacertFile
                }
            )
        ),
        ?_assertEqual(
            {error, ssl_cacertfile_is_not_readable},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => <<"/definitely/not/exists/ca.crt">>
                }
            )
        ),
        ?_assertEqual(
            {error, tls_client_certificates_not_supported},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    certfile => <<"/tmp/client.crt">>
                }
            )
        ),
        ?_assertEqual(
            {error, tls_client_certificates_not_supported},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    managed_certs => [#{bundle_name => <<"client">>}]
                }
            )
        ),
        ?_assertEqual(
            {error, ssl_server_name_indication_disabled},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    server_name_indication => disable
                }
            )
        ),
        ?_assertEqual(
            {error, ssl_server_name_indication_disabled},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    server_name_indication => "disable"
                }
            )
        ),
        ?_assertEqual(
            {error, ssl_server_name_indication_disabled},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    server_name_indication => <<"disable">>
                }
            )
        )
    ].

env_cacertfile_test() ->
    EnvName = "EMQX_DATALAYERS_TEST_CA_DIR",
    CacertFile = cacert_file(),
    Previous = os:getenv(EnvName),
    true = os:putenv(EnvName, unicode:characters_to_list(filename:dirname(CacertFile))),
    try
        Template = unicode:characters_to_binary([
            "${", EnvName, "}/", filename:basename(CacertFile)
        ]),
        ?assertEqual(
            #{
                tls_enable => true,
                tls_cacertfile => unicode:characters_to_binary(filename:absname(CacertFile)),
                verify => verify_peer,
                tls_server_name => <<"datalayers.example.com">>
            },
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => Template
                }
            )
        )
    after
        restore_env(EnvName, Previous)
    end.

relative_cacertfile_test() ->
    {ok, Contents} = file:read_file(cacert_file()),
    Filename =
        "datalayers-arrow-ca-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".pem",
    ok = file:write_file(Filename, Contents),
    try
        Relative = unicode:characters_to_binary(Filename),
        Expected = filename:absname(Relative),
        ?assertEqual(
            #{
                tls_enable => true,
                tls_cacertfile => Expected,
                verify => verify_peer,
                tls_server_name => <<"datalayers.example.com">>
            },
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => Relative
                }
            )
        )
    after
        file:delete(Filename)
    end.

invalid_cacertfile_test() ->
    Filename = filename:join(
        "/tmp",
        "datalayers-arrow-invalid-ca-" ++
            integer_to_list(erlang:unique_integer([positive])) ++ ".pem"
    ),
    ok = file:write_file(
        Filename,
        <<"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n">>
    ),
    try
        ?assertEqual(
            {error, ssl_cacertfile_is_not_pem},
            emqx_bridge_datalayers_arrow_flight_connector:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => unicode:characters_to_binary(Filename)
                }
            )
        )
    after
        file:delete(Filename)
    end.

connect_propagates_driver_error_test() ->
    meck:new(datalayers, [passthrough, no_link]),
    try
        meck:expect(
            datalayers,
            connect,
            fun(ClientOpts) ->
                ?assertNot(maps:is_key(database, ClientOpts)),
                ?assertEqual(<<"pass">>, maps:get(password, ClientOpts)),
                {error, {not_dispatched, <<"connect failed">>}}
            end
        ),
        ?assertEqual(
            {error, {not_dispatched, <<"connect failed">>}},
            emqx_bridge_datalayers_arrow_flight_connector:connect([
                {conn_config, #{
                    database => <<"db">>,
                    password => emqx_secret:wrap(<<"pass">>)
                }}
            ])
        ),
        ?assert(meck:validate(datalayers))
    after
        meck:unload(datalayers)
    end.

connect_propagates_database_error_test() ->
    meck:new(datalayers, [passthrough, no_link]),
    try
        Client = self(),
        meck:expect(datalayers, connect, fun(_) -> {ok, Client} end),
        meck:expect(
            datalayers,
            use_database,
            fun(_, _) -> {error, {not_dispatched, <<"database lookup failed">>}} end
        ),
        meck:expect(
            datalayers,
            stop,
            fun(Pid) ->
                ?assertEqual(Client, Pid),
                ok
            end
        ),
        ?assertEqual(
            {error,
                {use_database_failed, <<"db">>, {not_dispatched, <<"database lookup failed">>}}},
            emqx_bridge_datalayers_arrow_flight_connector:connect([
                {conn_config, #{
                    database => <<"db">>,
                    password => emqx_secret:wrap(<<"pass">>)
                }}
            ])
        ),
        ?assert(meck:validate(datalayers))
    after
        meck:unload(datalayers)
    end.

blank_credentials_test_() ->
    {setup,
        fun() ->
            meck:new(emqx_resource_pool, [passthrough, no_link]),
            meck:expect(
                emqx_resource_pool,
                start,
                fun(_, _, _) -> {error, {start_pool_failed, noproc, mocked}} end
            ),
            ok
        end,
        fun(_) -> meck:unload(emqx_resource_pool) end, fun(_) ->
            Base = #{
                server => <<"127.0.0.1:8360">>,
                pool_size => 8,
                ssl => #{enable => false},
                parameters => #{
                    driver_type => arrow_flight,
                    database => <<"db">>,
                    enable_prepared => true
                }
            },
            [
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base#{
                            parameters := (maps:get(parameters, Base))#{username => <<"user">>}
                        }
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base#{
                            parameters := (maps:get(parameters, Base))#{password => <<"pass">>}
                        }
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_database">>,
                        Base#{
                            parameters := maps:remove(
                                database,
                                maps:get(parameters, Base)
                            )
                        }
                    )
                )
            ]
        end}.

cacert_file() ->
    Dir = code:lib_dir(emqx_bridge_datalayers),
    filename:join([Dir, <<"test/emqx_bridge_datalayers_SUITE_data">>, <<"ca.crt">>]).

with_system_default_ca(Path, Fun) ->
    Previous = application:get_env(emqx, trusted_certs_path),
    ok = application:set_env(emqx, trusted_certs_path, binary_to_list(Path)),
    try
        Fun()
    after
        restore_trusted_certs(Previous)
    end.

without_system_default_ca(Fun) ->
    Previous = application:get_env(emqx, trusted_certs_path),
    ok = application:unset_env(emqx, trusted_certs_path),
    try
        Fun()
    after
        restore_trusted_certs(Previous)
    end.

restore_trusted_certs(undefined) ->
    application:unset_env(emqx, trusted_certs_path);
restore_trusted_certs({ok, Path}) ->
    application:set_env(emqx, trusted_certs_path, Path).

restore_env(EnvName, false) ->
    os:unsetenv(EnvName);
restore_env(EnvName, Value) ->
    os:putenv(EnvName, Value).

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(List) when is_list(List) ->
    unicode:characters_to_binary(List).
