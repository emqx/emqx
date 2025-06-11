%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_https_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(NAME, 'https:dashboard').
-define(HOST_HTTPS, "https://127.0.0.1:18084").
-define(HOST_HTTP, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").
-define(OVERVIEWS, [
    "alarms",
    "banned",
    "stats",
    "metrics",
    "listeners",
    "clients",
    "subscriptions"
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, Case, Config).
end_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, _TestCase, Config).

t_update_conf(init, Config) ->
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{<<"bind">> => 18084, <<"ssl_options">> => #{<<"depth">> => 5}},
                <<"http">> => #{<<"bind">> => 18083}
            }
        }
    },
    Apps = emqx_cth_suite_start(?FUNCTION_NAME, DashboardConf, Config),
    [{apps, Apps} | Config];
t_update_conf('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_update_conf(_Config) ->
    Headers = emqx_dashboard_SUITE:auth_header_(),
    {ok, Client1} = emqx_dashboard_SUITE:request_dashboard(
        get, https_api_path(["clients"]), Headers
    ),
    {ok, Client2} = emqx_dashboard_SUITE:request_dashboard(
        get, http_api_path(["clients"]), Headers
    ),
    Raw = emqx:get_raw_config([<<"dashboard">>]),
    ?assertEqual(
        5,
        emqx_utils_maps:deep_get(
            [<<"listeners">>, <<"https">>, <<"ssl_options">>, <<"depth">>], Raw
        )
    ),
    ?assertEqual(Client1, Client2),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"listeners">>, <<"https">>, <<"bind">>], Raw, 0
    ),
    ?check_trace(
        {_, {ok, _}} = ?wait_async_action(
            ?assertMatch({ok, _}, emqx:update_config([<<"dashboard">>], Raw1)),
            #{?snk_kind := regenerate_dispatch},
            1000
        ),
        fun(Trace) ->
            %% Don't start new listener, so is empty
            ?assertMatch([#{listeners := []}], ?of_kind(regenerate_dispatch, Trace))
        end
    ),
    ?assertEqual(Raw1, emqx:get_raw_config([<<"dashboard">>])),
    {ok, Client3} = emqx_dashboard_SUITE:request_dashboard(
        get, http_api_path(["clients"]), Headers
    ),
    ?assertEqual(Client1, Client3),
    ?assertMatch(
        {error,
            {failed_connect, [
                _,
                {inet, [inet], econnrefused}
            ]}},
        emqx_dashboard_SUITE:request_dashboard(get, https_api_path(["clients"]), Headers)
    ),
    %% reset
    ?check_trace(
        {_, {ok, _}} = ?wait_async_action(
            ?assertMatch({ok, _}, emqx:update_config([<<"dashboard">>], Raw)),
            #{?snk_kind := regenerate_dispatch},
            1000
        ),
        fun(Trace) ->
            %% start new listener('https:dashboard')
            ?assertMatch(
                [#{listeners := ['https:dashboard']}], ?of_kind(regenerate_dispatch, Trace)
            )
        end
    ),
    ?assertEqual(Raw, emqx:get_raw_config([<<"dashboard">>])),
    {ok, Client1} = emqx_dashboard_SUITE:request_dashboard(
        get, https_api_path(["clients"]), Headers
    ),
    {ok, Client2} = emqx_dashboard_SUITE:request_dashboard(
        get, http_api_path(["clients"]), Headers
    ),
    emqx_mgmt_api_test_util:end_suite([emqx_management]).

t_default_ssl_cert(init, Config) ->
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{<<"bind">> => 18084}
            }
        }
    },
    Apps = emqx_cth_suite_start(?FUNCTION_NAME, DashboardConf, Config),
    [{apps, Apps} | Config];
t_default_ssl_cert('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps).
t_default_ssl_cert(_Config) ->
    validate_https(512, default_ssl_cert(), verify_none).

t_compatibility_ssl_cert(init, Config) ->
    MaxConnection = 1000,
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{
                    bind => 18084,
                    cacertfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cacert.pem">>),
                    certfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cert.pem">>),
                    keyfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/key.pem">>),
                    max_connections => MaxConnection
                }
            }
        }
    },
    Apps = emqx_cth_suite_start(?FUNCTION_NAME, DashboardConf, Config),
    [{apps, Apps}, {max_connection, MaxConnection} | Config];
t_compatibility_ssl_cert('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.
t_compatibility_ssl_cert(Config) ->
    MaxConnection = ?config(max_connection, Config),
    validate_https(MaxConnection, default_ssl_cert(), verify_none).

t_normal_ssl_cert(init, Config) ->
    MaxConnection = 1024,
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{
                    <<"bind">> => 18084,
                    <<"ssl_options">> => #{
                        <<"cacertfile">> => naive_env_interpolation(
                            <<"${EMQX_ETC_DIR}/certs/cacert.pem">>
                        ),
                        <<"certfile">> => naive_env_interpolation(
                            <<"${EMQX_ETC_DIR}/certs/cert.pem">>
                        ),
                        <<"keyfile">> => naive_env_interpolation(
                            <<"${EMQX_ETC_DIR}/certs/key.pem">>
                        ),
                        <<"depth">> => 5
                    },
                    <<"max_connections">> => MaxConnection
                }
            }
        }
    },
    Apps = emqx_cth_suite_start(?FUNCTION_NAME, DashboardConf, Config),
    [{apps, Apps}, {max_connection, MaxConnection} | Config];
t_normal_ssl_cert('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.
t_normal_ssl_cert(Config) ->
    MaxConnection = ?config(max_connection, Config),
    validate_https(MaxConnection, default_ssl_cert(), verify_none).

t_verify_cacertfile(init, Config) ->
    MaxConnection = 1024,
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{
                    <<"bind">> => 18084,
                    <<"ssl_options">> => #{<<"cacertfile">> => <<"">>},
                    <<"max_connections">> => MaxConnection
                }
            }
        }
    },
    Apps = emqx_cth_suite_start(?FUNCTION_NAME, DashboardConf, Config),
    [{apps, Apps}, {max_connection, MaxConnection}, {dashboard_conf, DashboardConf} | Config];
t_verify_cacertfile('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.
t_verify_cacertfile(Config) ->
    MaxConnection = ?config(max_connection, Config),
    DefaultSSLCert = default_ssl_cert(),
    SSLCert = DefaultSSLCert#{cacertfile => <<"">>},

    %% validate with default #{verify => verify_none}
    validate_https(MaxConnection, SSLCert, verify_none),

    %% verify_peer but cacertfile is empty
    DashboardConf0 = ?config(dashboard_conf, Config),
    VerifyPeerConf1 = emqx_utils_maps:deep_put(
        [<<"dashboard">>, <<"listeners">>, <<"https">>, <<"ssl_options">>, <<"verify">>],
        DashboardConf0,
        <<"verify_peer">>
    ),
    {ok, _} = emqx:update_config([<<"dashboard">>], maps:get(<<"dashboard">>, VerifyPeerConf1)),
    ok = emqx_dashboard:stop_listeners(),
    ?assertMatch({error, [?NAME]}, emqx_dashboard:start_listeners()),

    %% verify_peer and cacertfile is ok.
    VerifyPeerConf2 = emqx_utils_maps:deep_put(
        [<<"dashboard">>, <<"listeners">>, <<"https">>, <<"ssl_options">>, <<"cacertfile">>],
        VerifyPeerConf1,
        naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cacert.pem">>)
    ),
    {ok, _} = emqx:update_config([<<"dashboard">>], maps:get(<<"dashboard">>, VerifyPeerConf2)),
    ok = emqx_dashboard:stop_listeners(),
    ok = emqx_dashboard:start_listeners(),
    %% we always test client with verify_none and no client cert is sent
    %% since the server is configured with verify_peer
    %% hence the expected observation on the client side is an error
    ErrorReason =
        try
            validate_https(MaxConnection, DefaultSSLCert, verify_peer)
        catch
            error:{https_client_error, Reason} ->
                Reason
        end,
    %% There seems to be a race-condition causing the return value to vary a bit
    case ErrorReason of
        socket_closed_remotely ->
            ok;
        {ssl_error, _SslSock, {tls_alert, {certificate_required, _}}} ->
            ok;
        Other ->
            throw({unexpected, Other})
    end.

t_bad_certfile(init, Config) ->
    erlang:process_flag(trap_exit, true),
    DashboardConf = #{
        <<"dashboard">> => #{
            <<"listeners">> => #{
                <<"https">> => #{
                    <<"bind">> => 18084,
                    <<"certfile">> => <<"${EMQX_ETC_DIR}/certs/not_found_cert.pem">>
                }
            }
        }
    },
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            {emqx_dashboard, #{
                config => DashboardConf,
                start => false
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [{apps, Apps} | Config];
t_bad_certfile('end', Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.
t_bad_certfile(_Config) ->
    ?assertMatch(
        {error, {emqx_dashboard, {[?NAME], _}}},
        application:ensure_all_started(emqx_dashboard)
    ).

validate_https(MaxConnection, SSLCert, Verify) ->
    assert_ranch_options(MaxConnection, SSLCert, Verify),
    assert_https_request().

assert_ranch_options(MaxConnections0, SSLCert, Verify) ->
    Middlewares = [emqx_dashboard_middleware, cowboy_router, cowboy_handler],
    [
        ?NAME,
        ranch_ssl,
        #{
            max_connections := MaxConnections,
            num_acceptors := _,
            socket_opts := SocketOpts
        },
        cowboy_tls,
        #{
            env := #{
                dispatch := {persistent_term, ?NAME},
                options := #{
                    name := ?NAME,
                    protocol := https,
                    protocol_options := #{proxy_header := false},
                    security := [#{basicAuth := []}, #{bearerAuth := []}],
                    swagger_global_spec := _
                }
            },
            middlewares := Middlewares,
            proxy_header := false
        }
    ] = ranch_server:get_listener_start_args(?NAME),
    ?assertEqual(MaxConnections0, MaxConnections),
    ?assert(lists:member(inet, SocketOpts), SocketOpts),
    #{
        backlog := 1024,
        ciphers := Ciphers,
        port := 18084,
        send_timeout := 10000,
        verify := Verify,
        versions := Versions
    } = SocketMaps = maps:from_list(SocketOpts -- [inet]),
    %% without tlsv1.1 tlsv1
    ?assertMatch(['tlsv1.3', 'tlsv1.2'], Versions),
    ?assert(Ciphers =/= []),
    maps:foreach(
        fun(K, ConfVal) ->
            case maps:find(K, SocketMaps) of
                {ok, File} -> ?assertEqual(naive_env_interpolation(ConfVal), File);
                error -> ?assertEqual(<<"">>, ConfVal)
            end
        end,
        SSLCert
    ),
    ?assertMatch(
        #{
            env := #{dispatch := {persistent_term, ?NAME}},
            middlewares := Middlewares,
            proxy_header := false
        },
        ranch:get_protocol_options(?NAME)
    ),
    ok.

assert_https_request() ->
    Headers = emqx_dashboard_SUITE:auth_header_(),
    lists:foreach(
        fun(Path) ->
            ApiPath = https_api_path([Path]),
            case emqx_dashboard_SUITE:request_dashboard(get, ApiPath, Headers) of
                {ok, _} -> ok;
                {error, Reason} -> error({https_client_error, Reason})
            end
        end,
        ?OVERVIEWS
    ).

https_api_path(Parts) ->
    ?HOST_HTTPS ++ filename:join([?BASE_PATH | Parts]).

http_api_path(Parts) ->
    ?HOST_HTTP ++ filename:join([?BASE_PATH | Parts]).

naive_env_interpolation(Str0) ->
    Str1 = emqx_schema:naive_env_interpolation(Str0),
    %% assert all envs are replaced
    ?assertNot(lists:member($$, Str1)),
    Str1.

default_ssl_cert() ->
    #{
        cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
        certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
        keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>
    }.

emqx_cth_suite_start(Case, DashboardConf, Config) ->
    emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(DashboardConf)
        ],
        #{work_dir => emqx_cth_suite:work_dir(Case, Config)}
    ).
