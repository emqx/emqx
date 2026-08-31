%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_listeners_update_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_schema.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-import(emqx_listeners, [current_conns/2, is_running/1]).

-define(LISTENERS, [listeners]).

all() ->
    [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(TestCase, Config) ->
    Init = emqx:get_raw_config(?LISTENERS),
    maybe_require_local_addr(TestCase, [{init_conf, Init} | Config]).

%% The bind portability warning needs an address that is really present on this
%% host. Skip the case when the host has none of the required family.
maybe_require_local_addr(t_bind_portability_warning, Config) ->
    require_local_addr(inet, Config);
maybe_require_local_addr(t_bind_portability_warning_ipv6, Config) ->
    require_local_addr(inet6, Config);
maybe_require_local_addr(_TestCase, Config) ->
    Config.

require_local_addr(Family, Config) ->
    case local_non_loopback_addr(Family) of
        undefined -> {skip, {no_non_loopback_address, Family}};
        IP -> [{local_addr, IP} | Config]
    end.

end_per_testcase(_TestCase, Config) ->
    Conf = ?config(init_conf, Config),
    {ok, _} = emqx:update_config(?LISTENERS, Conf),
    ok.

t_default_conf(Config) ->
    Profile = ?config(security_profile, Config),
    TcpBind = expected_default_bind(Profile, 1883),
    SslBind = expected_default_bind(Profile, 8883),
    WsBind = expected_default_bind(Profile, 8083),
    WssBind = expected_default_bind(Profile, 8084),
    RawTcpBind = format_raw_bind(TcpBind),
    RawSslBind = format_raw_bind(SslBind),
    RawWsBind = format_raw_bind(WsBind),
    RawWssBind = format_raw_bind(WssBind),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := RawTcpBind}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := RawSslBind}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := RawWsBind}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := RawWssBind}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{
            tcp := #{default := #{bind := TcpBind}},
            ssl := #{default := #{bind := SslBind}},
            ws := #{default := #{bind := WsBind}},
            wss := #{default := #{bind := WssBind}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_update_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"bind">>], Raw1, <<"127.0.0.1:8883">>
    ),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"ws">>, <<"default">>, <<"bind">>], Raw2, <<"0.0.0.0:8083">>
    ),
    Raw4 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"bind">>], Raw3, <<"127.0.0.1:8084">>
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:1883">>}},
            <<"ssl">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8883">>}},
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:8084">>}}
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    BindTcp = {{127, 0, 0, 1}, 1883},
    BindSsl = {{127, 0, 0, 1}, 8883},
    BindWs = {{0, 0, 0, 0}, 8083},
    BindWss = {{127, 0, 0, 1}, 8084},
    ?assertMatch(
        #{
            tcp := #{default := #{bind := BindTcp}},
            ssl := #{default := #{bind := BindSsl}},
            ws := #{default := #{bind := BindWs}},
            wss := #{default := #{bind := BindWss}}
        },
        emqx:get_config(?LISTENERS)
    ),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),

    ?assertEqual(0, current_conns(<<"tcp:default">>, BindTcp)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, BindSsl)),

    ?assertEqual({0, 0, 0, 0}, maps:get(ip, ranch:info('ws:default'))),
    ?assertEqual({127, 0, 0, 1}, maps:get(ip, ranch:info('wss:default'))),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.

t_update_conf_validate_access_rules(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    RawCorrectConf1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow all"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf1)),
    RawCorrectConf2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["deny all"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf2)),
    RawCorrectConf3 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow 10.0.1.0/24"]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, RawCorrectConf3)),
    RawIncorrectConf1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["xxx all"]
    ),
    ?assertMatch(
        {error, #{
            reason := <<"invalid_rule(s): xxx all">>,
            value := ["xxx all"],
            path := "listeners.tcp.default.access_rules",
            kind := validation_error,
            matched_type := "emqx:mqtt_tcp_listener"
        }},
        emqx:update_config(?LISTENERS, RawIncorrectConf1)
    ),
    RawIncorrectConf2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>], Raw, ["allow xxx"]
    ),
    ?assertMatch(
        {error, #{
            reason := <<"invalid_rule(s): allow xxx">>,
            value := ["allow xxx"],
            path := "listeners.tcp.default.access_rules",
            kind := validation_error,
            matched_type := "emqx:mqtt_tcp_listener"
        }},
        emqx:update_config(?LISTENERS, RawIncorrectConf2)
    ),
    ok.

t_update_conf_access_rules_split(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"access_rules">>],
        Raw,
        ["  allow all , deny all  , allow 10.0.1.0/24   "]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw1)),
    ?assertMatch(
        #{
            tcp := #{
                default := #{
                    access_rules := ["allow all", "deny all", "allow 10.0.1.0/24"]
                }
            }
        },
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_update_tcp_keepalive_conf(_Conf) ->
    Keepalive = <<"240,30,5">>,
    KeepaliveStr = binary_to_list(Keepalive),
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"tcp_options">>, <<"keepalive">>], Raw1, Keepalive
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw2)),
    ?assertMatch(
        #{
            <<"tcp">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:1883">>,
                    <<"tcp_options">> := #{<<"keepalive">> := Keepalive}
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{tcp := #{default := #{tcp_options := #{keepalive := KeepaliveStr}}}},
        emqx:get_config(?LISTENERS)
    ),
    Keepalive2 = <<" 241, 31, 6 ">>,
    KeepaliveStr2 = binary_to_list(Keepalive2),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"tcp_options">>, <<"keepalive">>], Raw1, Keepalive2
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw3)),
    ?assertMatch(
        #{
            <<"tcp">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:1883">>,
                    <<"tcp_options">> := #{<<"keepalive">> := Keepalive2}
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    ?assertMatch(
        #{tcp := #{default := #{tcp_options := #{keepalive := KeepaliveStr2}}}},
        emqx:get_config(?LISTENERS)
    ),
    ok.

t_tcp_change_parse_unit(_Conf) ->
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    test_change_parse_unit(tcp, Name, Port, #{
        hosts => [{{127, 0, 0, 1}, Port}]
    }).

t_ssl_change_parse_unit(_Conf) ->
    Name = ?FUNCTION_NAME,
    Port = emqx_common_test_helpers:select_free_port(tcp),
    test_change_parse_unit(ssl, Name, Port, #{
        hosts => [{{127, 0, 0, 1}, Port}],
        ssl => true,
        ssl_opts => [{verify, verify_none}]
    }).

test_change_parse_unit(Type, Name, Port, ClientOpts) ->
    ConfPath = ?LISTENERS ++ [Type, Name],
    DefaultRawConf = emqx:get_raw_config(?LISTENERS ++ [Type, default]),
    ListenerRawConf0 = maps:merge(
        DefaultRawConf#{
            <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
            <<"parse_unit">> => <<"chunk">>
        },
        maps:get(Type, #{
            tcp => #{<<"tcp_backend">> => <<"gen_tcp">>},
            ssl => #{}
        })
    ),
    ListenerRawConf1 = ListenerRawConf0#{
        <<"parse_unit">> => <<"frame">>
    },
    %% Update listener and verify `parse_unit` came into effect:
    ?assertMatch({ok, _}, emqx:update_config(ConfPath, {create, ListenerRawConf0})),
    Client1 = emqtt_connect(ClientOpts),
    pong = emqtt:ping(Client1),
    CState1 = emqx_cth_broker:connection_state(Client1),
    emqx_listeners:is_packet_parser_available(mqtt) andalso
        ?assertMatch(
            #{parser := Tuple} when element(1, Tuple) =:= options,
            CState1
        ),
    %% Restore original config and verify original `parse_unit` came into effect as well:
    ?assertMatch({ok, _}, emqx:update_config(ConfPath, {update, ListenerRawConf1})),
    Client2 = emqtt_connect(ClientOpts),
    pong = emqtt:ping(Client2),
    CState2 = emqx_cth_broker:connection_state(Client2),
    emqx_listeners:is_packet_parser_available(mqtt) andalso
        ?assertMatch(
            #{parser := Parser} when Parser =/= map_get(parser, CState1),
            CState2
        ),
    %% Existing connections should be preserved:
    pong = emqtt:ping(Client1),
    ok = emqtt:disconnect(Client1),
    pong = emqtt:ping(Client2),
    ok = emqtt:disconnect(Client2),
    %% Remove the listener:
    {ok, _} = emqx:update_config(ConfPath, ?TOMBSTONE_CONFIG_CHANGE_REQ).

t_update_empty_ssl_options_conf(_Conf) ->
    Raw = emqx:get_raw_config(?LISTENERS),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"default">>, <<"bind">>], Raw, <<"127.0.0.1:1883">>
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"bind">>], Raw1, <<"127.0.0.1:8883">>
    ),
    Raw3 = emqx_utils_maps:deep_put(
        [<<"ws">>, <<"default">>, <<"bind">>], Raw2, <<"0.0.0.0:8083">>
    ),
    Raw4 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"bind">>], Raw3, <<"127.0.0.1:8084">>
    ),
    Raw5 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"cacertfile">>], Raw4, <<"">>
    ),
    Raw6 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"ssl_options">>, <<"cacertfile">>], Raw5, <<"">>
    ),
    Raw7 = emqx_utils_maps:deep_put(
        [<<"wss">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>], Raw6, <<"">>
    ),
    Ciphers = <<"TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256 ">>,
    Raw8 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>],
        Raw7,
        Ciphers
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw8)),
    ?assertMatch(
        #{
            <<"tcp">> := #{<<"default">> := #{<<"bind">> := <<"127.0.0.1:1883">>}},
            <<"ssl">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:8883">>,
                    <<"ssl_options">> := #{
                        <<"cacertfile">> := <<"">>,
                        <<"ciphers">> := Ciphers
                    }
                }
            },
            <<"ws">> := #{<<"default">> := #{<<"bind">> := <<"0.0.0.0:8083">>}},
            <<"wss">> := #{
                <<"default">> := #{
                    <<"bind">> := <<"127.0.0.1:8084">>,
                    <<"ssl_options">> := #{
                        <<"cacertfile">> := <<"">>,
                        <<"ciphers">> := <<"">>
                    }
                }
            }
        },
        emqx:get_raw_config(?LISTENERS)
    ),
    BindTcp = {{127, 0, 0, 1}, 1883},
    BindSsl = {{127, 0, 0, 1}, 8883},
    BindWs = {{0, 0, 0, 0}, 8083},
    BindWss = {{127, 0, 0, 1}, 8084},
    ?assertMatch(
        #{
            tcp := #{default := #{bind := BindTcp}},
            ssl := #{
                default := #{
                    bind := BindSsl,
                    ssl_options := #{
                        cacertfile := <<"">>,
                        ciphers := ["TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"]
                    }
                }
            },
            ws := #{default := #{bind := BindWs}},
            wss := #{
                default := #{
                    bind := BindWss,
                    ssl_options := #{
                        cacertfile := <<"">>,
                        ciphers := []
                    }
                }
            }
        },
        emqx:get_config(?LISTENERS)
    ),
    ?assertError(not_found, current_conns(<<"tcp:default">>, {{0, 0, 0, 0}, 1883})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),

    ?assertEqual(0, current_conns(<<"tcp:default">>, BindTcp)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, BindSsl)),

    ?assertEqual({0, 0, 0, 0}, maps:get(ip, ranch:info('ws:default'))),
    ?assertEqual({127, 0, 0, 1}, maps:get(ip, ranch:info('wss:default'))),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),

    Raw9 = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"ciphers">>], Raw7, [
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256"
        ]
    ),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw9)),

    BadRaw = emqx_utils_maps:deep_put(
        [<<"ssl">>, <<"default">>, <<"ssl_options">>, <<"keyfile">>], Raw4, <<"">>
    ),
    ?assertMatch(
        {error,
            {bad_ssl_config, #{
                reason := pem_file_path_or_string_is_required,
                which_option := <<"keyfile">>
            }}},
        emqx:update_config(?LISTENERS, BadRaw)
    ),
    ok.

t_add_delete_conf(Config) ->
    DefaultSslBind = expected_default_bind(?config(security_profile, Config), 8883),
    Raw = emqx:get_raw_config(?LISTENERS),
    %% add
    #{<<"tcp">> := #{<<"default">> := Tcp}} = Raw,
    NewBind = <<"127.0.0.1:1987">>,
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"new">>], Raw, Tcp#{<<"bind">> => NewBind}),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw2)),
    ?assertEqual(0, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertError(not_found, current_conns(<<"ssl:default">>, {{0, 0, 0, 0}, 8883})),
    %% deleted
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertError(not_found, current_conns(<<"tcp:new">>, {{127, 0, 0, 1}, 1987})),
    ?assertEqual(0, current_conns(<<"ssl:default">>, DefaultSslBind)),
    ok.

t_delete_default_conf(Config) ->
    Profile = ?config(security_profile, Config),
    DefaultTcpBind = expected_default_bind(Profile, 1883),
    DefaultSslBind = expected_default_bind(Profile, 8883),
    Raw = emqx:get_raw_config(?LISTENERS),
    %% delete default listeners
    Raw1 = emqx_utils_maps:deep_put([<<"tcp">>, <<"default">>], Raw, ?TOMBSTONE_VALUE),
    Raw2 = emqx_utils_maps:deep_put([<<"ssl">>, <<"default">>], Raw1, ?TOMBSTONE_VALUE),
    Raw3 = emqx_utils_maps:deep_put([<<"ws">>, <<"default">>], Raw2, ?TOMBSTONE_VALUE),
    Raw4 = emqx_utils_maps:deep_put([<<"wss">>, <<"default">>], Raw3, ?TOMBSTONE_VALUE),
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw4)),
    ?assertError(not_found, current_conns(<<"tcp:default">>, DefaultTcpBind)),
    ?assertError(not_found, current_conns(<<"ssl:default">>, DefaultSslBind)),
    ?assertMatch({error, not_found}, is_running('ws:default')),
    ?assertMatch({error, not_found}, is_running('wss:default')),

    %% reset
    ?assertMatch({ok, _}, emqx:update_config(?LISTENERS, Raw)),
    ?assertEqual(0, current_conns(<<"tcp:default">>, DefaultTcpBind)),
    ?assertEqual(0, current_conns(<<"ssl:default">>, DefaultSslBind)),
    ?assert(is_running('ws:default')),
    ?assert(is_running('wss:default')),
    ok.

-doc """
Binding a listener to a real local non-loopback IPv4 address on a single-node
cluster logs a portability warning; the config change succeeds and the
listener runs.
""".
t_bind_portability_warning(Config) ->
    IP = ?config(local_addr, Config),
    #{<<"tcp">> := #{<<"default">> := Tcp}} = emqx:get_raw_config(?LISTENERS),
    Bind = format_bind({IP, 21883}),
    ConfPath = [listeners, tcp, portability],
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                emqx:update_config(ConfPath, {create, Tcp#{<<"bind">> => Bind}})
            ),
            ?assertEqual(0, current_conns(<<"tcp:portability">>, {IP, 21883})),
            {ok, _} = emqx:update_config(ConfPath, ?TOMBSTONE_CONFIG_CHANGE_REQ)
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{level := warning, bind := Bind}],
                ?of_kind(listener_bind_portability_log, Trace)
            )
        end
    ).

-doc """
A local non-loopback IPv6 address logs the same portability warning as an
IPv4 one. The listener is disabled, so the case does not depend on the host
being able to bind IPv6.
""".
t_bind_portability_warning_ipv6(Config) ->
    IP = ?config(local_addr, Config),
    Bind = format_bind({IP, 21888}),
    ?assertMatch(
        [#{level := warning, bind := Bind}],
        bind_portability_logs(Bind, #{<<"enable">> => false})
    ).

-doc """
Binding a listener to an IPv4 address that is not on the local interfaces logs
an error; the config change is not rejected.
""".
t_bind_portability_error_not_local(_Config) ->
    %% TEST-NET-1 (RFC 5737) address: never present on the local interfaces.
    Bind = <<"192.0.2.7:21884">>,
    ?assertMatch(
        [#{level := error, bind := Bind}],
        bind_portability_logs(Bind, #{<<"enable">> => false})
    ).

-doc """
Binding a listener to an IPv6 address that is not on the local interfaces logs
an error; the config change is not rejected.
""".
t_bind_portability_error_not_local_ipv6(_Config) ->
    %% Documentation prefix (RFC 3849): never present on the local interfaces.
    Bind = <<"[2001:db8::7]:21887">>,
    ?assertMatch(
        [#{level := error, bind := Bind}],
        bind_portability_logs(Bind, #{<<"enable">> => false})
    ).

-doc """
Generic bind addresses log nothing: the IPv4 and IPv6 wildcard, IPv4 and IPv6
loopback, an address in 127.0.0.0/8 other than 127.0.0.1, and a bare port.
""".
t_bind_portability_no_log_for_generic(_Config) ->
    Binds = [
        <<"0.0.0.0:21885">>,
        <<"127.0.0.1:21885">>,
        <<"127.0.0.53:21885">>,
        <<"[::]:21885">>,
        <<"[::1]:21885">>,
        21885
    ],
    lists:foreach(
        fun(Bind) ->
            ?assertEqual(
                [],
                bind_portability_logs(Bind, #{<<"enable">> => false}),
                #{bind => Bind}
            )
        end,
        Binds
    ).

-doc """
Decision matrix of the log severity: a local address on a single-node cluster
is a warning; a non-local address, or any host-specific address in a cluster
with more than one node, is an error. Checked for an IPv4 and an IPv6 bind.
""".
t_bind_portability_decision(_Config) ->
    Cases = [
        {true, true, warning},
        {true, false, error},
        {false, true, error},
        {false, false, error}
    ],
    Binds = [
        {{{192, 0, 2, 7}, 1883}, <<"192.0.2.7:1883">>},
        {{{16#2001, 16#db8, 0, 0, 0, 0, 0, 7}, 1883}, <<"[2001:db8::7]:1883">>}
    ],
    lists:foreach(
        fun({Bind, BindStr}) ->
            lists:foreach(
                fun({IsLocal, IsSingleNode, ExpectedLevel}) ->
                    ?check_trace(
                        ok = emqx_listeners:do_log_bind_portability(
                            'tcp:x', Bind, IsLocal, IsSingleNode
                        ),
                        fun(Trace) ->
                            ?assertMatch(
                                [#{level := ExpectedLevel, bind := BindStr}],
                                ?of_kind(listener_bind_portability_log, Trace),
                                #{is_local => IsLocal, is_single_node => IsSingleNode}
                            )
                        end
                    )
                end,
                Cases
            )
        end,
        Binds
    ).

%%

%% Create a listener with the given bind, then delete it. Returns the
%% portability log events the config change produced. The events are emitted
%% synchronously by `post_config_update', so the trace is complete when
%% `update_config' returns.
bind_portability_logs(Bind, Overrides) ->
    #{<<"tcp">> := #{<<"default">> := Tcp}} = emqx:get_raw_config(?LISTENERS),
    ConfPath = [listeners, tcp, portability],
    Conf = maps:merge(Tcp#{<<"bind">> => Bind}, Overrides),
    ok = snabbkaffe:start_trace(),
    try
        ?assertMatch({ok, _}, emqx:update_config(ConfPath, {create, Conf})),
        {ok, _} = emqx:update_config(ConfPath, ?TOMBSTONE_CONFIG_CHANGE_REQ),
        ?of_kind(listener_bind_portability_log, snabbkaffe:collect_trace())
    after
        snabbkaffe:stop()
    end.

local_non_loopback_addr(Family) ->
    Size =
        case Family of
            inet -> 4;
            inet6 -> 8
        end,
    {ok, IfAddrs} = inet:getifaddrs(),
    Addrs = [
        Addr
     || {_Name, Opts} <- IfAddrs,
        {addr, Addr} <- Opts,
        tuple_size(Addr) =:= Size,
        not is_generic_addr(Addr)
    ],
    case Addrs of
        [] -> undefined;
        [Addr | _] -> Addr
    end.

is_generic_addr({127, _, _, _}) -> true;
is_generic_addr({0, 0, 0, 0}) -> true;
is_generic_addr({0, 0, 0, 0, 0, 0, 0, 0}) -> true;
is_generic_addr({0, 0, 0, 0, 0, 0, 0, 1}) -> true;
is_generic_addr(_) -> false.

emqtt_connect(Opts) ->
    case emqtt:start_link(Opts) of
        {ok, Client} ->
            true = erlang:unlink(Client),
            case emqtt:connect(Client) of
                {ok, _} -> Client;
                {error, Reason} -> error(Reason, [Opts])
            end;
        {error, Reason} ->
            error(Reason, [Opts])
    end.

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).

%% Schema defaults are static bare ports; the profile is applied at
%% listener start, and the keyed accessors translate binds the same way.
expected_default_bind(_Profile, Port) -> Port.

format_raw_bind(Port) when is_integer(Port) -> Port;
format_raw_bind({{127, 0, 0, 1}, Port}) -> <<"127.0.0.1:", (integer_to_binary(Port))/binary>>.
