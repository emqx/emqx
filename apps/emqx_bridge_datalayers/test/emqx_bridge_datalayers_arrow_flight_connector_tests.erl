%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_arrow_flight_connector_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CONNECTOR, emqx_bridge_datalayers_arrow_flight_connector).

ssl_opts_test_() ->
    CacertFile = bin(cacert_file()),
    [
        ?_assertEqual(
            #{},
            ?CONNECTOR:ssl_opts(
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
            ?CONNECTOR:ssl_opts(
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
            ?CONNECTOR:ssl_opts(
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
        ?_assertEqual(
            {error, ssl_cacertfile_is_required},
            ?CONNECTOR:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{enable => true, verify => verify_peer}
            )
        ),
        ?_assertEqual(
            {error, verify_none_not_supported},
            ?CONNECTOR:ssl_opts(
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
            {error, ssl_cacertfile_must_be_absolute},
            ?CONNECTOR:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => <<"ca.crt">>
                }
            )
        ),
        ?_assertEqual(
            {error, ssl_cacertfile_is_not_readable},
            ?CONNECTOR:ssl_opts(
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
            ?CONNECTOR:ssl_opts(
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
            {error, ssl_server_name_indication_disabled},
            ?CONNECTOR:ssl_opts(
                <<"test:ssl_opts">>,
                <<"datalayers.example.com">>,
                #{
                    enable => true,
                    verify => verify_peer,
                    cacertfile => CacertFile,
                    server_name_indication => disable
                }
            )
        )
    ].

connect_propagates_driver_error_test() ->
    meck:new(datalayers, [passthrough, no_link]),
    try
        meck:expect(
            datalayers,
            connect,
            fun(ClientOpts) ->
                ?assertEqual(
                    #{password => <<"pass">>},
                    ClientOpts
                ),
                {error, {not_dispatched, <<"connect failed">>}}
            end
        ),
        ?assertEqual(
            {error, {not_dispatched, <<"connect failed">>}},
            ?CONNECTOR:connect([
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

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(List) when is_list(List) ->
    iolist_to_binary(List).
