%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx_managed_certs.hrl").

-define(SERVER_KEY_PASSWORD, "sErve7r8Key$!").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(MK_NAME(SUFFIX), mk_name(?FUNCTION_NAME, SUFFIX)).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    generate_tls_certs(Config),
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start([quicer, emqx], #{work_dir => WorkDir}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) when
    Case =:= t_start_stop_listeners;
    Case =:= t_restart_listeners;
    Case =:= t_wait_for_stop_listeners;
    Case =:= t_restart_listeners_with_hibernate_after_disabled
->
    ok = emqx_listeners:stop(),
    ct:timetrap({seconds, 30}),
    Config;
init_per_testcase(_, Config) ->
    ok = emqx_listeners:start(),
    ct:timetrap({seconds, 30}),
    Config.

end_per_testcase(_, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

t_start_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    ?assertException(error, _, emqx_listeners:start_listener(ws, {"127.0.0.1", 8083}, #{})),
    ok = emqx_listeners:stop().

t_wait_for_stop_listeners(_) ->
    ct:timetrap({seconds, 120}),
    ok = emqx_listeners:start(),
    meck:new([cowboy], [passthrough, no_history, no_link]),
    %% mock stop_listener return ok but listen port is still open
    meck:expect(cowboy, stop_listener, fun(_) -> ok end),
    List = [
        {<<"ws:default">>, {"127.0.0.1", 8083}},
        {<<"wss:default">>, {"127.0.0.1", 8084}}
    ],
    lists:foreach(
        fun({Id, ListenerOn}) ->
            Start = erlang:system_time(seconds),
            ok = emqx_listeners:stop_listener(Id),
            ?assertEqual(timeout, emqx_listeners:wait_listener_stopped(ListenerOn)),
            End = erlang:system_time(seconds),
            ?assert(End - Start >= 9, "wait_listener_stopped should wait at least 9 seconds")
        end,
        List
    ),
    meck:unload(cowboy),
    lists:foreach(
        fun({Id, ListenerOn}) ->
            ok = emqx_listeners:stop_listener(Id),
            ?assertEqual(ok, emqx_listeners:wait_listener_stopped(ListenerOn))
        end,
        List
    ),
    ok = emqx_listeners:stop(),
    ok.

t_restart_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop().

t_restart_listeners_with_hibernate_after_disabled(_Config) ->
    OldLConf = emqx_config:get([listeners]),
    maps:foreach(
        fun(LType, Listeners) ->
            maps:foreach(
                fun(Name, Opts) ->
                    case maps:is_key(ssl_options, Opts) of
                        true ->
                            emqx_config:put(
                                [
                                    listeners,
                                    LType,
                                    Name,
                                    ssl_options,
                                    hibernate_after
                                ],
                                undefined
                            );
                        _ ->
                            skip
                    end
                end,
                Listeners
            )
        end,
        OldLConf
    ),
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop(),
    emqx_config:put([listeners], OldLConf).

t_client_attr_as_mountpoint(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    ListenerConf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"mountpoint">> => <<"groups/${client_attrs.ns}/">>
    },
    {ok, Compiled} = emqx_variform:compile("nth(1,tokens(clientid,'-'))"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"ns">>
        }
    ]),
    with_listener(tcp, attr_as_moutpoint, ListenerConf, fun() ->
        {ok, Client} = emqtt:start_link(#{
            hosts => [{"127.0.0.1", Port}],
            clientid => <<"abc-123">>
        }),
        unlink(Client),
        {ok, _} = emqtt:connect(Client),
        TopicPrefix = atom_to_binary(?FUNCTION_NAME),
        SubTopic = <<TopicPrefix/binary, "/#">>,
        MatchTopic = <<"groups/abc/", TopicPrefix/binary, "/1">>,
        {ok, _, [1]} = emqtt:subscribe(Client, SubTopic, 1),
        ?assertMatch([_], emqx_router:match_routes(MatchTopic)),
        emqtt:stop(Client)
    end),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    ok.

t_current_conns_tcp(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"max_connections">> => 42
    },
    with_listener(tcp, curconns, Conf, fun() ->
        ?assertEqual(
            0,
            emqx_listeners:current_conns('tcp:curconns', {{127, 0, 0, 1}, Port})
        )
    end).

t_tcp_chunk_parsing_conn(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"tcp_backend">> => <<"gen_tcp">>,
        <<"parse_unit">> => <<"chunk">>
    },
    with_listener(tcp, ?FUNCTION_NAME, Conf, fun() ->
        Client = emqtt_connect_tcp({127, 0, 0, 1}, Port),
        pong = emqtt:ping(Client),
        CState = emqx_cth_broker:connection_state(Client),
        ?assertMatch(#{listener := {tcp, ?FUNCTION_NAME}}, CState),
        ?assertMatch(#{parser := Tuple} when element(1, Tuple) =:= options, CState)
    end).

t_tcp_socket_conn(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"tcp_backend">> => <<"socket">>
    },
    with_listener(tcp, ?FUNCTION_NAME, Conf, fun() ->
        Client = emqtt_connect_tcp({127, 0, 0, 1}, Port),
        pong = emqtt:ping(Client),
        ?assertEqual(
            emqx_socket_connection,
            emqx_cth_broker:connection_info(connmod, Client)
        )
    end).

t_ssl_chunk_parsing_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server.key")
        },
        <<"parse_unit">> => <<"chunk">>
    },
    with_listener(ssl, ?FUNCTION_NAME, Conf, fun() ->
        Client = emqtt_connect_ssl({127, 0, 0, 1}, Port, [{verify, verify_none}]),
        pong = emqtt:ping(Client),
        ClientId = proplists:get_value(clientid, emqtt:info(Client)),
        [CPid] = emqx_cm:lookup_channels(ClientId),
        CState = emqx_connection:get_state(CPid),
        ?assertMatch(#{listener := {ssl, ?FUNCTION_NAME}}, CState),
        ?assertMatch(#{parser := Tuple} when element(1, Tuple) =:= options, CState)
    end).

t_wss_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server.key")
        }
    },
    with_listener(wss, wssconn, Conf, fun() ->
        {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{verify, verify_none}], 1000),
        ok = ssl:close(Socket)
    end).

-doc """
Smoke test for using managed certificates (global ns) in a WSS listener.
""".
t_wss_managed_certs_global(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(ssl),
    BundleName = <<"bundle">>,
    {ok, _} = generate_and_upload_managed_certs(?global_ns, BundleName, #{}),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    },
    with_listener(wss, ?FUNCTION_NAME, Conf, fun() ->
        C = emqtt_connect_wss("127.0.0.1", Port, [{verify, verify_none}]),
        ok = emqtt:stop(C)
    end),
    ok.

-doc """
Smoke test for using managed certificates (managed ns) in a WSS listener.
""".
t_wss_managed_certs_ns(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Namespace = <<"some_ns">>,
    BundleName = <<"bundle">>,
    {ok, _} = generate_and_upload_managed_certs(Namespace, BundleName, #{}),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"managed_certs">> => #{
                <<"namespace">> => Namespace,
                <<"bundle_name">> => BundleName
            }
        }
    },
    with_listener(wss, ?FUNCTION_NAME, Conf, fun() ->
        C = emqtt_connect_wss("127.0.0.1", Port, [{verify, verify_none}]),
        ok = emqtt:stop(C)
    end),
    ok.

-doc """
Smoke test for using managed certificates (global ns) in a SSL listener.
""".
t_ssl_managed_certs_global(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(ssl),
    BundleName = <<"bundle">>,
    {ok, _} = generate_and_upload_managed_certs(?global_ns, BundleName, #{}),
    LConf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
        <<"ssl_options">> => #{<<"managed_certs">> => #{<<"bundle_name">> => BundleName}}
    },
    with_listener(ssl, ?FUNCTION_NAME, LConf, fun() ->
        {ok, SSLSocket} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}]),
        ssl:close(SSLSocket)
    end),
    ok.

-doc """
Smoke test for using managed certificates (managed ns) in a SSL listener.
""".
t_ssl_managed_certs_ns(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Namespace = <<"some_ns">>,
    BundleName = <<"bundle">>,
    {ok, _} = generate_and_upload_managed_certs(Namespace, BundleName, #{}),
    LConf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
        <<"ssl_options">> => #{
            <<"managed_certs">> => #{
                <<"namespace">> => Namespace,
                <<"bundle_name">> => BundleName
            }
        }
    },
    with_listener(ssl, ?FUNCTION_NAME, LConf, fun() ->
        {ok, SSLSocket} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}]),
        ssl:close(SSLSocket)
    end),
    ok.

-doc """
Smoke test for using managed certificates (global ns) in a SSL listener, using a password
protected private key file.
""".
t_ssl_managed_certs_password(Config) ->
    PrivDir0 = ?config(priv_dir, Config),
    PrivDir = filename:join([PrivDir0, ?FUNCTION_NAME]),
    ok = filelib:ensure_path(PrivDir),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    BundleName = <<"bundle">>,
    Password = <<"$ecr3tP@sç"/utf8>>,
    {ok, #{
        ca := CAPEM,
        mk_cert_key_fn := MkCertKeyFn
    }} =
        generate_and_upload_managed_certs(?global_ns, BundleName, #{password => Password}),
    CA = filename:join([PrivDir, "ca.pem"]),
    ok = file:write_file(CA, CAPEM),
    #{key_pem := ClientKeyPEM, cert_pem := ClientCertPEM} = MkCertKeyFn(#{}),
    ClientKey = filename:join(PrivDir, "client.key"),
    ClientCert = filename:join(PrivDir, "client.pem"),
    ok = file:write_file(ClientKey, ClientKeyPEM),
    ok = file:write_file(ClientCert, ClientCertPEM),
    LConf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
        <<"ssl_options">> => #{
            <<"verify">> => <<"verify_none">>,
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    },
    with_listener(ssl, ?FUNCTION_NAME, LConf, fun() ->
        C = emqtt_connect_ssl("127.0.0.1", Port, [
            {verify, verify_none},
            {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]},
            {cacertfile, CA},
            {certfile, ClientCert},
            {keyfile, ClientKey}
        ]),
        emqtt:stop(C)
    end),
    ok.

-doc """
Checks the behavior of referencing a managed cert bundle that is somehow broken.

- Inexistent.
- Password file cannot be read.

The problems above should throw an error at runtime when starting the listener.

- Bad directory/file permissions.
- Incomplete bundle (e.g., missing cert chain).

The problems above just log an error and fail the client connection, but do not prevent
the listener from starting.
""".
t_ssl_managed_certs_broken_reference(_Config) ->
    Type = ssl,
    Name = ?MK_NAME("1"),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Namespace = ?global_ns,
    BundleName = atom_to_binary(Name),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    },
    %% Inexistent bundle
    ?assertMatch(
        {error, #{
            error := <<"failed_to_resolve_managed_certs">>,
            reason := "No such file or directory",
            dir := _,
            namespace := _,
            bundle := _
        }},
        emqx:update_config([listeners, Type, Name], {create, Conf})
    ),
    %% Password file cannot be read.
    Password = <<"$ecr3tP@sç"/utf8>>,
    {ok, _} = generate_and_upload_managed_certs(Namespace, BundleName, #{password => Password}),
    {ok, #{?FILE_KIND_KEY_PASSWORD := #{path := KeyPasswordPath}}} =
        emqx_managed_certs:list_managed_files(Namespace, BundleName),
    %% No read permission
    ok = file:change_mode(KeyPasswordPath, 8#333),
    %% Sanity/CI check
    %% In CI, these tests are currently run by `root`, which can still read the files even
    %% after the above `chmod`...
    CheckReadError = fun() ->
        ?assertMatch(
            {error, #{
                error := <<"failed_to_read_managed_file">>,
                reason := "Permission denied",
                path := _,
                namespace := _,
                bundle := _
            }},
            emqx:update_config([listeners, Type, Name], {create, Conf})
        )
    end,
    case file:read_file(KeyPasswordPath) of
        {error, eacces} ->
            CheckReadError();
        {ok, _} ->
            %% running as root
            emqx_common_test_helpers:with_mock(
                file,
                read_file,
                fun
                    (Path) when Path == KeyPasswordPath ->
                        {error, eacces};
                    (Path) ->
                        meck:passthrough([Path])
                end,
                #{meck_opts => [no_history, passthrough, unstick]},
                CheckReadError
            )
    end,
    ok = file:change_mode(KeyPasswordPath, 8#666),
    ok = file:delete(KeyPasswordPath),

    %% Bad directory/file permissions.
    {ok, _} = generate_and_upload_managed_certs(Namespace, BundleName, #{}),
    {ok, #{?FILE_KIND_KEY := #{path := KeyPath}}} =
        emqx_managed_certs:list_managed_files(Namespace, BundleName),
    %% No read permission
    ok = file:change_mode(KeyPath, 8#333),
    CheckRuntimeReadError = fun() ->
        ?assertMatch(
            {ok, _},
            emqx:update_config([listeners, Type, Name], {create, Conf})
        ),
        ?assertError(
            _,
            emqtt_connect_ssl({127, 0, 0, 1}, Port, [{verify, verify_none}])
        )
    end,
    %% Sanity check
    %% In CI, these tests are currently run by `root`, which can still read the files even
    %% after the above `chmod`...
    case file:read_file(KeyPath) of
        {error, eacces} ->
            CheckRuntimeReadError();
        {ok, _} ->
            %% running as root
            emqx_common_test_helpers:with_mock(
                file,
                read_file,
                fun
                    (Path) when Path == KeyPath ->
                        {error, eacces};
                    (Path) ->
                        meck:passthrough([Path])
                end,
                #{meck_opts => [no_history, passthrough, unstick]},
                CheckRuntimeReadError
            )
    end,
    {ok, _} = emqx:remove_config([listeners, Type, Name]),

    %% Incomplete bundle (e.g., missing cert chain).
    {ok, _} = generate_and_upload_managed_certs(Namespace, BundleName, #{}),
    ok = emqx_managed_certs:delete_managed_file(Namespace, BundleName, ?FILE_KIND_CHAIN),
    Name2 = ?MK_NAME("2"),
    Port2 = emqx_common_test_helpers:select_free_port(ssl),
    Conf2 = Conf#{<<"bind">> := format_bind({{127, 0, 0, 1}, Port2})},
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                emqx:update_config([listeners, Type, Name2], {create, Conf2})
            ),
            ?assertMatch(
                {error, _},
                ssl:connect("127.0.0.1", Port2, [{verify, verify_none}], 1_000)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind("missing_required_managed_cert_files", Trace)),
            ok
        end
    ),

    snabbkaffe:stop(),
    {ok, _} = emqx:remove_config([listeners, Type, Name2]),
    ok = emqx_listeners:stop(),

    ok.

t_quic_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(quic),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key")
        }
    },
    with_listener(quic, ?FUNCTION_NAME, Conf, fun() ->
        {ok, Conn} = quicer:connect(
            {127, 0, 0, 1},
            Port,
            [
                {verify, verify_none},
                {alpn, ["mqtt"]}
            ],
            1000
        ),
        ok = quicer:close_connection(Conn)
    end).

-doc """
Smoke test for using managed certificates (global ns) in a QUIC listener.
""".
t_quic_managed_certs_global(_TCConfig) ->
    Port = emqx_common_test_helpers:select_free_port(quic),
    BundleName = <<"bundle">>,
    {ok, _} = generate_and_upload_managed_certs(?global_ns, BundleName, #{}),
    {ok, #{
        ?FILE_KIND_CA := #{path := CAPath0},
        ?FILE_KIND_CHAIN := #{path := ChainPath0},
        ?FILE_KIND_KEY := #{path := KeyPath0}
    }} = emqx_managed_certs:list_managed_files(?global_ns, BundleName),
    CAPath = str(CAPath0),
    ChainPath = str(ChainPath0),
    KeyPath = str(KeyPath0),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    },
    ?check_trace(
        with_listener(quic, ?FUNCTION_NAME, Conf, fun() ->
            {ok, Conn} = quicer:connect(
                {127, 0, 0, 1},
                Port,
                [
                    {verify, verify_none},
                    {alpn, ["mqtt"]}
                ],
                1000
            ),
            ok = quicer:close_connection(Conn)
        end),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        listen_opts := #{
                            cacertfile := CAPath,
                            certfile := ChainPath,
                            keyfile := KeyPath
                        }
                    }
                ],
                ?of_kind("quic_listener_opts", Trace),
                #{
                    cacertfile => CAPath,
                    certfile => ChainPath,
                    keyfile => KeyPath
                }
            ),
            ok
        end
    ),
    ok.

t_ssl_password_cert(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    SSLOptsPWD = #{
        <<"password">> => ?SERVER_KEY_PASSWORD,
        <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
        <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
        <<"keyfile">> => filename:join(PrivDir, "server-password.key")
    },
    LConf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
        <<"ssl_options">> => SSLOptsPWD
    },
    with_listener(ssl, ?FUNCTION_NAME, LConf, fun() ->
        {ok, SSLSocket} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}]),
        ssl:close(SSLSocket)
    end).

t_ssl_update_opts(Config) ->
    ct:timetrap({seconds, 120}),
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ssl, Name, Conf, fun() ->
        %% Client connects successfully.
        ct:pal("attempting successful connection"),
        C1 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: another set of cert/key files.
        ct:pal("updating config"),
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ct:pal("attempting connection with unknown CA"),
        ?assertError(
            {tls_alert, {unknown_ca, _}},
            emqtt_connect_ssl(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        C2 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        try
            emqtt_connect_ssl(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
            ]),
            ct:fail("l ~b: unexpected success", [?LINE])
        catch
            error:{ssl_error, _Socket, {tls_alert, {certificate_required, _}}} ->
                ok;
            error:closed ->
                ok;
            error:connack_timeout ->
                ok;
            K:E:S ->
                error({unexpected_exception, {K, E, S}})
        end,

        C3 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_ssl_update_versions(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Name = ?FUNCTION_NAME,
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
            <<"certfile">> => filename:join(PrivDir, "server.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {cacertfile, filename:join(PrivDir, "ca-next.pem")},
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    with_listener(ssl, Name, Conf, fun() ->
        %% Client connects successfully.
        ct:pal("attempting successful connection"),
        C1 = emqtt_connect_ssl(Host, Port, ClientSSLOpts),

        %% Change the listener SSL configuration: force TLSv1.3.
        ct:pal("updating config"),
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"versions">> => [<<"tlsv1.3">>]
                }
            }}
        ),

        C2 = emqtt_connect_ssl(Host, Port, ClientSSLOpts),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"versions">> => [<<"tlsv1.2">>, <<"tlsv1.3">>],
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true,
                    <<"client_renegotiation">> => false
                }
            }}
        ),

        C3 = emqtt_connect_ssl(Host, Port, [
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_wss_update_opts(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(wss, Name, Conf, fun() ->
        %% Start a client.
        ct:pal("attempting successful connection"),
        C1 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")}
            | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration.
        %% 1. Another set of (password protected) cert/key files.
        %% 2. Require peer certificate.
        ct:pal("changing config"),
        {ok, _} = emqx:update_config(
            [listeners, wss, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ct:pal("attempting connection with unknown CA"),
        ?assertError(
            {down, {shutdown, {tls_alert, {unknown_ca, _}}}},
            emqtt_connect_wss(Host, Port, [
                {cacerts, public_key:cacerts_get()}
                | ClientSSLOpts
            ])
        ),

        ct:pal("attempting connection with another CA"),
        C2 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")}
            | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        ct:pal("updating config"),
        {ok, _} = emqx:update_config(
            [listeners, wss, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        ct:pal("asserting certificate required error"),
        CertReqErr =
            try
                emqtt_connect_wss(Host, Port, [
                    {cacertfile, filename:join(PrivDir, "ca-next.pem")}
                    | ClientSSLOpts
                ]),
                {error, <<"didn't raise any errors!">>}
            catch
                error:Reason ->
                    Reason
            end,
        case CertReqErr of
            %% these errors may race
            {ws_upgrade_failed, {closed, {error, {tls_alert, {certificate_required, _}}}}} ->
                ok;
            {ws_upgrade_failed, {error, {tls_alert, {certificate_required, _}}}} ->
                ok;
            {ws_upgrade_failed, {error, closed}} ->
                ok;
            _ ->
                error({unexpected_error, CertReqErr})
        end,

        ct:pal("connecting client with new ca"),
        C3 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ct:pal("checking clients are still alive"),
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_quic_update_opts(Config) ->
    ListenerType = quic,
    ConnectFun = connect_fun(ListenerType),
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ListenerType),
    ok = emqx_config:put_zone_conf(?FUNCTION_NAME, [mqtt, max_topic_levels], 2),

    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ListenerType, Name, Conf, fun() ->
        %% Client connects successfully.
        C1 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: another set of cert/key files.
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ?assertError(
            {transport_down, #{error := _, status := Status}} when
                ((Status =:= bad_certificate orelse
                    Status =:= cert_untrusted_root orelse
                    Status =:= unknown_certificate orelse
                    Status =:= handshake_failure)),
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        C2 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        ?assertExceptionOneOf(
            {exit, _},
            {error, _},
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
            ])
        ),

        C3 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Change the listener port and zone
        NewPort = emqx_common_test_helpers:select_free_port(ListenerType),
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"bind">> => format_bind({Host, NewPort}),
                <<"zone">> => ?FUNCTION_NAME
            }}
        ),

        %% Connect to old port fail
        ?assertExceptionOneOf(
            {exit, _},
            {error, _},
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")},
                {certfile, filename:join(PrivDir, "client.pem")},
                {keyfile, filename:join(PrivDir, "client.key")}
                | ClientSSLOpts
            ])
        ),

        %% Connect to new port successfully.
        C4 = ConnectFun(Host, NewPort, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),
        ?assertEqual(pong, emqtt:ping(C4)),

        ?assertMatch({ok, _, [?RC_GRANTED_QOS_1]}, emqtt:subscribe(C1, <<"test/2/3">>, 1)),
        ?assertMatch({ok, _, [?RC_UNSPECIFIED_ERROR]}, emqtt:subscribe(C4, <<"test/2/3">>, 1)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3),
        ok = emqtt:stop(C4)
    end).

t_quic_update_opts_fail(Config) ->
    ListenerType = quic,
    ConnectFun = connect_fun(ListenerType),
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ListenerType),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ListenerType, Name, Conf, fun() ->
        %% GIVEN: an working Listener that client could connect to.
        C1 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% WHEN: reload the listener with invalid SSL options (certfile and keyfile missmatch).
        UpdateResult1 = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server-password.key")
                }
            }}
        ),

        %% THEN: Reload failed but old listener is rollbacked.
        ?assertMatch(
            {error, {post_config_update, emqx_listeners, {rollbacked, {error, tls_error}}}},
            UpdateResult1
        ),

        %% THEN: Client with old TLS options could still connect
        C2 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% WHEN: Change the listener SSL configuration again
        UpdateResult2 = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),
        %% THEN: update should success
        ?assertMatch({ok, _}, UpdateResult2),

        %% THEN: Client with old TLS options could not connect
        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ?assertError(
            {transport_down, #{error := _, status := Status}} when
                ((Status =:= bad_certificate orelse
                    Status =:= cert_untrusted_root orelse
                    Status =:= unknown_certificate orelse
                    Status =:= handshake_failure)),
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        %% THEN: Client with new TLS options could connect
        C3 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_max_packet_size_update(_Config) ->
    case emqx_listeners:is_packet_parser_available(mqtt) of
        true ->
            test_max_packet_size_update();
        false ->
            ok
    end.

test_max_packet_size_update() ->
    Tester = self(),
    meck:new(emqx_listeners, [passthrough]),
    meck:expect(
        emqx_listeners,
        update_listener_for_zone_changes,
        fun(Type, Name, Conf) ->
            Tester ! {update, Type, Name, Conf},
            meck:passthrough([Type, Name, Conf])
        end
    ),
    KeyPath = [mqtt, max_packet_size],
    MaxPacketSize = emqx_config:get_zone_conf(default, KeyPath),
    emqx_config:put_zone_conf(default, KeyPath, MaxPacketSize + 1),
    %% two updates, one for tcp, one for ssl, without order
    ?assertReceive({update, Type, default, _} when Type =:= tcp orelse Type =:= ssl, 1000),
    ?assertReceive({update, Type, default, _} when Type =:= tcp orelse Type =:= ssl, 1000),
    ?assertNotReceive({update, ws, default, _}, 200),
    %% update without diff should not result in listener update
    emqx_config:put_zone_conf(default, KeyPath, MaxPacketSize + 1),
    ?assertNotReceive({update, _, default, _}, 200),
    %% restore the original value
    emqx_config:put_zone_conf(default, KeyPath, MaxPacketSize),
    ok.

t_symlink_certs(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Cacertfile = filename:join(PrivDir, "ca-next.pem"),
    Certfile = filename:join(PrivDir, "server.pem"),
    Keyfile = filename:join(PrivDir, "server.key"),
    CacertfileSymlink = filename:join(PrivDir, "ca-next-symlink.pem"),
    CertfileSymlink = filename:join(PrivDir, "server-symlink.pem"),
    KeyfileSymlink = filename:join(PrivDir, "server-symlink.key"),
    ok = file:make_symlink(Cacertfile, CacertfileSymlink),
    ok = file:make_symlink(Certfile, CertfileSymlink),
    ok = file:make_symlink(Keyfile, KeyfileSymlink),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => CacertfileSymlink,
            <<"certfile">> => CertfileSymlink,
            <<"keyfile">> => KeyfileSymlink,
            <<"verify">> => <<"verify_peer">>
        }
    },
    Name = ?FUNCTION_NAME,
    Type = ssl,
    with_listener(Type, Name, Conf, fun() ->
        ClientSSLOpts = [
            {verify, verify_peer},
            {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
        ],
        C1 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),
        emqtt:stop(C1),
        ok
    end),
    ok.

with_listener(Type, Name, Config, Then) ->
    {ok, _} = emqx:update_config([listeners, Type, Name], {create, Config}),
    try
        Then()
    after
        ok = emqx_listeners:stop(),
        emqx:remove_config([listeners, Type, Name])
    end.

emqtt_connect_tcp(Host, Port) ->
    emqtt_connect(fun emqtt:connect/1, #{
        host => Host,
        port => Port,
        connect_timeout => 500
    }).

emqtt_connect_ssl(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ssl => true,
        ssl_opts => SSLOpts
    }).

emqtt_connect_quic(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:quic_connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ssl => true,
        ssl_opts => SSLOpts
    }).

emqtt_connect_wss(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ws_transport_options => [
            {protocols, [http]},
            {transport, tls},
            {tls_opts, SSLOpts}
        ]
    }).

emqtt_connect(Connect, Opts) ->
    case emqtt:start_link(Opts) of
        {ok, Client} ->
            true = erlang:unlink(Client),
            case Connect(Client) of
                {ok, _} -> Client;
                {error, Reason} -> error(Reason, [Opts])
            end;
        {error, Reason} ->
            error(Reason, [Opts])
    end.

t_format_bind(_) ->
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(1883))
    ),
    ?assertEqual(
        "0.0.0.0:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "[::]:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0, 0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "127.0.0.1:1883",
        lists:flatten(emqx_listeners:format_bind({{127, 0, 0, 1}, 1883}))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind("1883"))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(":1883"))
    ).

generate_tls_certs(Config) ->
    PrivDir = ?config(priv_dir, Config),
    emqx_common_test_helpers:gen_ca(PrivDir, "ca"),
    emqx_common_test_helpers:gen_ca(PrivDir, "ca-next"),
    emqx_common_test_helpers:gen_host_cert("server", "ca-next", PrivDir, #{}),
    emqx_common_test_helpers:gen_host_cert("client", "ca-next", PrivDir, #{}),
    emqx_common_test_helpers:gen_host_cert("server-password", "ca", PrivDir, #{
        password => ?SERVER_KEY_PASSWORD
    }).

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).

connect_fun(ssl) ->
    fun emqtt_connect_ssl/3;
connect_fun(quic) ->
    fun emqtt_connect_quic/3;
connect_fun(wss) ->
    fun emqtt_connect_wss/3.

generate_cert_pem_bundle(Opts0) ->
    #{
        cert := CertRoot,
        key := KeyRoot,
        cert_pem := CAPEM,
        key_pem := CAKeyPEM
    } = emqx_cth_tls:gen_cert_pem(#{key => ec, issuer => root}),
    Opts = maps:with([password], Opts0),
    #{
        cert_pem := CertPEM,
        key_pem := KeyPEM
    } = emqx_cth_tls:gen_cert_pem(Opts#{key => ec, issuer => {CertRoot, KeyRoot}}),
    #{
        files => #{
            ?FILE_KIND_CA => CAPEM,
            ?FILE_KIND_CHAIN => CertPEM,
            ?FILE_KIND_KEY => KeyPEM
        },
        ca_key_pem => CAKeyPEM,
        mk_cert_key_fn => fun(Opts1) ->
            emqx_cth_tls:gen_cert_pem(Opts1#{key => ec, issuer => {CertRoot, KeyRoot}})
        end
    }.

generate_and_upload_managed_certs(Namespace, BundleName, Opts) ->
    #{
        files := Files0,
        ca_key_pem := CAKeyPEM,
        mk_cert_key_fn := MkCertKeyFn
    } = generate_cert_pem_bundle(Opts),
    #{?FILE_KIND_CA := CAPEM} = Files0,
    Files =
        case Opts of
            #{password := Password} ->
                Files0#{?FILE_KIND_KEY_PASSWORD => Password};
            _ ->
                Files0
        end,
    ok = emqx_managed_certs:add_managed_files(Namespace, BundleName, Files),
    on_exit(fun() -> ok = emqx_managed_certs:delete_bundle(Namespace, BundleName) end),
    {ok, #{mk_cert_key_fn => MkCertKeyFn, ca => CAPEM, ca_key => CAKeyPEM}}.

mk_name(FnName, Suffix) ->
    binary_to_atom(iolist_to_binary([atom_to_binary(FnName), Suffix])).

str(X) -> emqx_utils_conv:str(X).
