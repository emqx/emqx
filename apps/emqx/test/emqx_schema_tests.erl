%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_schema_tests).

-include_lib("eunit/include/eunit.hrl").

ssl_opts_dtls_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(
        #{
            versions => dtls_all_available
        },
        false
    ),
    Checked = validate(Sc, #{<<"versions">> => [<<"dtlsv1.2">>, <<"dtlsv1">>]}),
    ?assertMatch(
        #{
            versions := ['dtlsv1.2', 'dtlsv1'],
            ciphers := []
        },
        Checked
    ).

ssl_opts_tls_1_3_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.3">>]}),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := [],
            handshake_timeout := _
        },
        Checked
    ).

ssl_opts_tls_for_ranch_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, true),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.3">>]}),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := [],
            handshake_timeout := _
        },
        Checked
    ).

ssl_opts_cipher_array_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{
        <<"versions">> => [<<"tlsv1.3">>],
        <<"ciphers">> => [
            <<"TLS_AES_256_GCM_SHA384">>,
            <<"ECDHE-ECDSA-AES256-GCM-SHA384">>
        ]
    }),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := ["TLS_AES_256_GCM_SHA384", "ECDHE-ECDSA-AES256-GCM-SHA384"]
        },
        Checked
    ).

ssl_opts_cipher_comma_separated_string_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{
        <<"versions">> => [<<"tlsv1.3">>],
        <<"ciphers">> => <<"TLS_AES_256_GCM_SHA384,ECDHE-ECDSA-AES256-GCM-SHA384">>
    }),
    ?assertMatch(
        #{
            versions := ['tlsv1.3'],
            ciphers := ["TLS_AES_256_GCM_SHA384", "ECDHE-ECDSA-AES256-GCM-SHA384"]
        },
        Checked
    ).

ssl_opts_tls_psk_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Checked = validate(Sc, #{<<"versions">> => [<<"tlsv1.2">>]}),
    ?assertMatch(#{versions := ['tlsv1.2']}, Checked).

ssl_opts_version_gap_test_() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    RanchSc = emqx_schema:server_ssl_opts_schema(#{}, true),
    Reason = "Using multiple versions that include tlsv1.3 but exclude tlsv1.2 is not allowed",
    [
        ?_assertThrow(
            {_, [#{kind := validation_error, reason := Reason}]},
            validate(S, #{<<"versions">> => [<<"tlsv1.1">>, <<"tlsv1.3">>]})
        )
     || S <- [Sc, RanchSc]
    ].

ssl_opts_cert_depth_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Reason = #{expected => "non_neg_integer()"},
    ?assertThrow(
        {_Sc, [#{kind := validation_error, reason := Reason}]},
        validate(Sc, #{<<"depth">> => -1})
    ).

bad_cipher_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Reason = {bad_ciphers, ["foo"]},
    ?assertThrow(
        {_Sc, [#{kind := validation_error, reason := Reason}]},
        validate(Sc, #{
            <<"versions">> => [<<"tlsv1.2">>],
            <<"ciphers">> => [<<"foo">>]
        })
    ),
    ok.

fail_if_no_peer_cert_test_() ->
    Sc = #{
        roots => [mqtt_ssl_listener],
        fields => #{mqtt_ssl_listener => emqx_schema:fields("mqtt_ssl_listener")}
    },
    Opts = #{atom_key => false, required => false},
    OptsAtomKey = #{atom_key => true, required => false},
    InvalidConf = #{
        <<"bind">> => <<"0.0.0.0:9883">>,
        <<"ssl_options">> => #{
            <<"fail_if_no_peer_cert">> => true,
            <<"verify">> => <<"verify_none">>
        }
    },
    InvalidListener = #{<<"mqtt_ssl_listener">> => InvalidConf},
    ValidListener = #{
        <<"mqtt_ssl_listener">> => InvalidConf#{
            <<"ssl_options">> =>
                #{
                    <<"fail_if_no_peer_cert">> => true,
                    <<"verify">> => <<"verify_peer">>
                }
        }
    },
    ValidListener1 = #{
        <<"mqtt_ssl_listener">> => InvalidConf#{
            <<"ssl_options">> =>
                #{
                    <<"fail_if_no_peer_cert">> => false,
                    <<"verify">> => <<"verify_none">>
                }
        }
    },
    Reason = "verify must be verify_peer when fail_if_no_peer_cert is true",
    [
        ?_assertThrow(
            {_Sc, [#{kind := validation_error, reason := Reason}]},
            hocon_tconf:check_plain(Sc, InvalidListener, Opts)
        ),
        ?_assertThrow(
            {_Sc, [#{kind := validation_error, reason := Reason}]},
            hocon_tconf:check_plain(Sc, InvalidListener, OptsAtomKey)
        ),
        ?_assertMatch(
            #{mqtt_ssl_listener := #{}},
            hocon_tconf:check_plain(Sc, ValidListener, OptsAtomKey)
        ),
        ?_assertMatch(
            #{mqtt_ssl_listener := #{}},
            hocon_tconf:check_plain(Sc, ValidListener1, OptsAtomKey)
        ),
        ?_assertMatch(
            #{<<"mqtt_ssl_listener">> := #{}},
            hocon_tconf:check_plain(Sc, ValidListener, Opts)
        ),
        ?_assertMatch(
            #{<<"mqtt_ssl_listener">> := #{}},
            hocon_tconf:check_plain(Sc, ValidListener1, Opts)
        )
    ].

validate(Schema, Data0) ->
    Sc = #{
        roots => [ssl_opts],
        fields => #{ssl_opts => Schema}
    },
    Data = Data0#{
        cacertfile => <<"cacertfile">>,
        certfile => <<"certfile">>,
        keyfile => <<"keyfile">>
    },
    #{ssl_opts := Checked} =
        hocon_tconf:check_plain(
            Sc,
            #{<<"ssl_opts">> => Data},
            #{atom_key => true}
        ),
    Checked.

ciphers_schema_test() ->
    Sc = emqx_schema:ciphers_schema(undefined),
    WSc = #{roots => [{ciphers, Sc}]},
    ?assertThrow(
        {_, [#{kind := validation_error}]},
        hocon_tconf:check_plain(WSc, #{<<"ciphers">> => <<"foo,bar">>})
    ).

bad_tls_version_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(#{}, false),
    Reason = {unsupported_tls_versions, [foo]},
    ?assertThrow(
        {_Sc, [#{kind := validation_error, reason := Reason}]},
        validate(Sc, #{<<"versions">> => [<<"foo">>]})
    ),
    ok.

ssl_opts_gc_after_handshake_test_rancher_listener_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(
        #{
            gc_after_handshake => false
        },
        _IsRanchListener = true
    ),
    ?assertThrow(
        {_Sc, [
            #{
                kind := validation_error,
                reason := unknown_fields,
                unknown := "gc_after_handshake"
            }
        ]},
        validate(Sc, #{<<"gc_after_handshake">> => true})
    ),
    ok.

ssl_opts_gc_after_handshake_test_not_rancher_listener_test() ->
    Sc = emqx_schema:server_ssl_opts_schema(
        #{
            gc_after_handshake => false
        },
        _IsRanchListener = false
    ),
    Checked = validate(Sc, #{<<"gc_after_handshake">> => <<"true">>}),
    ?assertMatch(
        #{
            gc_after_handshake := true
        },
        Checked
    ),
    ok.

to_ip_port_test_() ->
    Ip = fun emqx_schema:to_ip_port/1,
    [
        ?_assertEqual({ok, 80}, Ip("80")),
        ?_assertEqual({ok, 80}, Ip(":80")),
        ?_assertEqual({error, bad_ip_port}, Ip("localhost:80")),
        ?_assertEqual({ok, {{127, 0, 0, 1}, 80}}, Ip("127.0.0.1:80")),
        ?_assertEqual({error, bad_ip_port}, Ip("$:1900")),
        ?_assertMatch({ok, {_, 1883}}, Ip("[::1]:1883")),
        ?_assertMatch({ok, {_, 1883}}, Ip("::1:1883")),
        ?_assertMatch({ok, {_, 1883}}, Ip(":::1883"))
    ].

-define(T(CASE, EXPR), {CASE, fun() -> EXPR end}).

parse_server_test_() ->
    DefaultPort = ?LINE,
    DefaultOpts = #{default_port => DefaultPort},
    Parse2 = fun(Value0, Opts) ->
        Value = emqx_schema:convert_servers(Value0),
        Validator = emqx_schema:servers_validator(Opts, _Required = true),
        try
            Result = emqx_schema:parse_servers(Value, Opts),
            ?assertEqual(ok, Validator(Value)),
            Result
        catch
            throw:Throw ->
                %% assert validator throws the same exception
                ?assertThrow(Throw, Validator(Value)),
                %% and then let the test code validate the exception
                throw(Throw)
        end
    end,
    Parse = fun(Value) -> Parse2(Value, DefaultOpts) end,
    HoconParse = fun(Str0) ->
        {ok, Map} = hocon:binary(Str0),
        Str = emqx_schema:convert_servers(Map),
        Parse(Str)
    end,
    [
        ?T(
            "single server, binary, no port",
            ?assertEqual(
                [#{hostname => "localhost", port => DefaultPort}],
                Parse(<<"localhost">>)
            )
        ),
        ?T(
            "single server, string, no port",
            ?assertEqual(
                [#{hostname => "localhost", port => DefaultPort}],
                Parse("localhost")
            )
        ),
        ?T(
            "single server, list(string), no port",
            ?assertEqual(
                [#{hostname => "localhost", port => DefaultPort}],
                Parse(["localhost"])
            )
        ),
        ?T(
            "single server, list(binary), no port",
            ?assertEqual(
                [#{hostname => "localhost", port => DefaultPort}],
                Parse([<<"localhost">>])
            )
        ),
        ?T(
            "single server, binary, with port",
            ?assertEqual(
                [#{hostname => "localhost", port => 9999}],
                Parse(<<"localhost:9999">>)
            )
        ),
        ?T(
            "single server, list(string), with port",
            ?assertEqual(
                [#{hostname => "localhost", port => 9999}],
                Parse(["localhost:9999"])
            )
        ),
        ?T(
            "single server, string, with port",
            ?assertEqual(
                [#{hostname => "localhost", port => 9999}],
                Parse("localhost:9999")
            )
        ),
        ?T(
            "single server, list(binary), with port",
            ?assertEqual(
                [#{hostname => "localhost", port => 9999}],
                Parse([<<"localhost:9999">>])
            )
        ),
        ?T(
            "multiple servers, string, no port",
            ?assertEqual(
                [
                    #{hostname => "host1", port => DefaultPort},
                    #{hostname => "host2", port => DefaultPort}
                ],
                Parse("host1, host2")
            )
        ),
        ?T(
            "multiple servers, binary, no port",
            ?assertEqual(
                [
                    #{hostname => "host1", port => DefaultPort},
                    #{hostname => "host2", port => DefaultPort}
                ],
                Parse(<<"host1, host2,,,">>)
            )
        ),
        ?T(
            "multiple servers, list(string), no port",
            ?assertEqual(
                [
                    #{hostname => "host1", port => DefaultPort},
                    #{hostname => "host2", port => DefaultPort}
                ],
                Parse(["host1", "host2"])
            )
        ),
        ?T(
            "multiple servers, list(binary), no port",
            ?assertEqual(
                [
                    #{hostname => "host1", port => DefaultPort},
                    #{hostname => "host2", port => DefaultPort}
                ],
                Parse([<<"host1">>, <<"host2">>])
            )
        ),
        ?T(
            "multiple servers, string, with port",
            ?assertEqual(
                [#{hostname => "host1", port => 1234}, #{hostname => "host2", port => 2345}],
                Parse("host1:1234, host2:2345")
            )
        ),
        ?T(
            "multiple servers, binary, with port",
            ?assertEqual(
                [#{hostname => "host1", port => 1234}, #{hostname => "host2", port => 2345}],
                Parse(<<"host1:1234, host2:2345, ">>)
            )
        ),
        ?T(
            "multiple servers, list(string), with port",
            ?assertEqual(
                [#{hostname => "host1", port => 1234}, #{hostname => "host2", port => 2345}],
                Parse([" host1:1234 ", "host2:2345"])
            )
        ),
        ?T(
            "multiple servers, list(binary), with port",
            ?assertEqual(
                [#{hostname => "host1", port => 1234}, #{hostname => "host2", port => 2345}],
                Parse([<<"host1:1234">>, <<"host2:2345">>])
            )
        ),
        ?T(
            "unexpected multiple servers",
            ?assertThrow(
                "expecting_one_host_but_got: 2",
                emqx_schema:parse_server(<<"host1:1234, host2:1234">>, #{default_port => 1})
            )
        ),
        ?T(
            "multiple servers without ports invalid string list",
            ?assertThrow(
                "hostname_has_space",
                Parse2(["host1 host2"], #{no_port => true})
            )
        ),
        ?T(
            "multiple servers without ports invalid binary list",
            ?assertThrow(
                "hostname_has_space",
                Parse2([<<"host1 host2">>], #{no_port => true})
            )
        ),
        ?T(
            "multiple servers without port, mixed list(binary|string)",
            ?assertEqual(
                [#{hostname => "host1"}, #{hostname => "host2"}],
                Parse2([<<"host1">>, "host2"], #{no_port => true})
            )
        ),
        ?T(
            "no default port, missing port number in config",
            ?assertThrow(
                "missing_port_number",
                emqx_schema:parse_server(<<"a">>, #{})
            )
        ),
        ?T(
            "empty binary string",
            ?assertEqual(
                undefined,
                emqx_schema:parse_server(<<>>, #{no_port => true})
            )
        ),
        ?T(
            "empty array",
            ?assertEqual(
                undefined,
                emqx_schema:parse_servers([], #{no_port => true})
            )
        ),
        ?T(
            "empty binary array",
            ?assertThrow(
                "bad_host_port",
                emqx_schema:parse_servers([<<>>], #{no_port => true})
            )
        ),
        ?T(
            "HOCON value undefined",
            ?assertEqual(
                undefined,
                emqx_schema:parse_server(undefined, #{no_port => true})
            )
        ),
        ?T(
            "single server map",
            ?assertEqual(
                [#{hostname => "host1.domain", port => 1234}],
                HoconParse("host1.domain:1234")
            )
        ),
        ?T(
            "multiple servers map",
            ?assertEqual(
                [
                    #{hostname => "host1.domain", port => 1234},
                    #{hostname => "host2.domain", port => 2345},
                    #{hostname => "host3.domain", port => 3456}
                ],
                HoconParse("host1.domain:1234,host2.domain:2345,host3.domain:3456")
            )
        ),
        ?T(
            "no port expected valid port",
            ?assertThrow(
                "not_expecting_port_number",
                emqx_schema:parse_server("localhost:80", #{no_port => true})
            )
        ),
        ?T(
            "no port expected invalid port",
            ?assertThrow(
                "not_expecting_port_number",
                emqx_schema:parse_server("localhost:notaport", #{no_port => true})
            )
        ),

        ?T(
            "bad hostname",
            ?assertThrow(
                "expecting_hostname_but_got_a_number",
                emqx_schema:parse_server(":80", #{default_port => 80})
            )
        ),
        ?T(
            "bad port",
            ?assertThrow(
                "bad_port_number",
                emqx_schema:parse_server("host:33x", #{default_port => 33})
            )
        ),
        ?T(
            "bad host with port",
            ?assertThrow(
                "bad_host_port",
                emqx_schema:parse_server("host:name:80", #{default_port => 80})
            )
        ),
        ?T(
            "bad schema",
            ?assertError(
                "bad_schema",
                emqx_schema:parse_server("whatever", #{default_port => 10, no_port => true})
            )
        ),
        ?T(
            "scheme, hostname and port",
            ?assertEqual(
                #{scheme => "pulsar+ssl", hostname => "host", port => 6651},
                emqx_schema:parse_server(
                    "pulsar+ssl://host:6651",
                    #{
                        default_port => 6650,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "scheme and hostname, default port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host", port => 6650},
                emqx_schema:parse_server(
                    "pulsar://host",
                    #{
                        default_port => 6650,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "scheme and hostname, no port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host"},
                emqx_schema:parse_server(
                    "pulsar://host",
                    #{
                        no_port => true,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "scheme and hostname, missing port",
            ?assertThrow(
                "missing_port_number",
                emqx_schema:parse_server(
                    "pulsar://host",
                    #{
                        no_port => false,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "hostname, default scheme, no default port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host"},
                emqx_schema:parse_server(
                    "host",
                    #{
                        default_scheme => "pulsar",
                        no_port => true,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "hostname, default scheme, default port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host", port => 6650},
                emqx_schema:parse_server(
                    "host",
                    #{
                        default_port => 6650,
                        default_scheme => "pulsar",
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "just hostname, expecting missing scheme",
            ?assertThrow(
                "missing_scheme",
                emqx_schema:parse_server(
                    "host",
                    #{
                        no_port => true,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "hostname, default scheme, defined port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host", port => 6651},
                emqx_schema:parse_server(
                    "host:6651",
                    #{
                        default_port => 6650,
                        default_scheme => "pulsar",
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "inconsistent scheme opts",
            ?assertError(
                "bad_schema",
                emqx_schema:parse_server(
                    "pulsar+ssl://host:6651",
                    #{
                        default_port => 6650,
                        default_scheme => "something",
                        supported_schemes => ["not", "supported"]
                    }
                )
            )
        ),
        ?T(
            "hostname, default scheme, defined port",
            ?assertEqual(
                #{scheme => "pulsar", hostname => "host", port => 6651},
                emqx_schema:parse_server(
                    "host:6651",
                    #{
                        default_port => 6650,
                        default_scheme => "pulsar",
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        ),
        ?T(
            "unsupported scheme",
            ?assertThrow(
                "unsupported_scheme",
                emqx_schema:parse_server(
                    "pulsar+quic://host:6651",
                    #{
                        default_port => 6650,
                        supported_schemes => ["pulsar"]
                    }
                )
            )
        ),
        ?T(
            "multiple hostnames with schemes (1)",
            ?assertEqual(
                [
                    #{scheme => "pulsar", hostname => "host", port => 6649},
                    #{scheme => "pulsar+ssl", hostname => "other.host", port => 6651},
                    #{scheme => "pulsar", hostname => "yet.another", port => 6650}
                ],
                emqx_schema:parse_servers(
                    "pulsar://host:6649, pulsar+ssl://other.host:6651,pulsar://yet.another",
                    #{
                        default_port => 6650,
                        supported_schemes => ["pulsar", "pulsar+ssl"]
                    }
                )
            )
        )
    ].

servers_validator_test() ->
    Required = emqx_schema:servers_validator(#{}, true),
    NotRequired = emqx_schema:servers_validator(#{}, false),
    ?assertThrow("cannot_be_empty", Required("")),
    ?assertThrow("cannot_be_empty", Required(<<>>)),
    ?assertThrow("cannot_be_empty", NotRequired("")),
    ?assertThrow("cannot_be_empty", NotRequired(<<>>)),
    ?assertThrow("cannot_be_empty", Required(undefined)),
    ?assertEqual(ok, NotRequired(undefined)),
    ?assertEqual(ok, NotRequired("undefined")),
    ok.

converter_invalid_input_test() ->
    ?assertEqual(undefined, emqx_schema:convert_servers(undefined)),
    %% 'foo: bar' is a valid HOCON value, but 'bar' is not a port number
    ?assertThrow("bad_host_port", emqx_schema:convert_servers(#{foo => bar})).

password_converter_test() ->
    ?assertEqual(undefined, emqx_schema:password_converter(undefined, #{})),
    ?assertEqual(<<"123">>, emqx_schema:password_converter(123, #{})),
    ?assertEqual(<<"123">>, emqx_schema:password_converter(<<"123">>, #{})),
    ?assertThrow("must_quote", emqx_schema:password_converter(foobar, #{})),
    ok.

-define(MQTT(B, M), #{<<"keepalive_backoff">> => B, <<"keepalive_multiplier">> => M}).

keepalive_convert_test() ->
    ?assertEqual(undefined, emqx_schema:mqtt_converter(undefined, #{})),
    DefaultBackoff = 0.75,
    DefaultMultiplier = 1.5,
    Default = ?MQTT(DefaultBackoff, DefaultMultiplier),
    ?assertEqual(Default, emqx_schema:mqtt_converter(Default, #{})),
    ?assertEqual(?MQTT(1.5, 3), emqx_schema:mqtt_converter(?MQTT(1.5, 3), #{})),
    ?assertEqual(
        ?MQTT(DefaultBackoff, 3), emqx_schema:mqtt_converter(?MQTT(DefaultBackoff, 3), #{})
    ),
    ?assertEqual(?MQTT(1, 2), emqx_schema:mqtt_converter(?MQTT(1, DefaultMultiplier), #{})),
    ?assertEqual(?MQTT(1.5, 3), emqx_schema:mqtt_converter(?MQTT(1.5, 3), #{})),

    ?assertEqual(#{}, emqx_schema:mqtt_converter(#{}, #{})),
    ?assertEqual(
        #{<<"keepalive_backoff">> => 1.5, <<"keepalive_multiplier">> => 3.0},
        emqx_schema:mqtt_converter(#{<<"keepalive_backoff">> => 1.5}, #{})
    ),
    ?assertEqual(
        #{<<"keepalive_multiplier">> => 5.0},
        emqx_schema:mqtt_converter(#{<<"keepalive_multiplier">> => 5.0}, #{})
    ),
    ?assertEqual(
        #{
            <<"keepalive_backoff">> => DefaultBackoff,
            <<"keepalive_multiplier">> => DefaultMultiplier
        },
        emqx_schema:mqtt_converter(#{<<"keepalive_backoff">> => DefaultBackoff}, #{})
    ),
    ?assertEqual(
        #{<<"keepalive_multiplier">> => DefaultMultiplier},
        emqx_schema:mqtt_converter(#{<<"keepalive_multiplier">> => DefaultMultiplier}, #{})
    ),
    ok.

url_type_test_() ->
    [
        ?_assertEqual(
            {ok, <<"http://some.server/">>},
            typerefl:from_string(emqx_schema:url(), <<"http://some.server/">>)
        ),
        ?_assertEqual(
            {ok, <<"http://192.168.0.1/">>},
            typerefl:from_string(emqx_schema:url(), <<"http://192.168.0.1">>)
        ),
        ?_assertEqual(
            {ok, <<"http://some.server/">>},
            typerefl:from_string(emqx_schema:url(), "http://some.server/")
        ),
        ?_assertEqual(
            {ok, <<"http://some.server/">>},
            typerefl:from_string(emqx_schema:url(), <<"http://some.server">>)
        ),
        ?_assertEqual(
            {ok, <<"http://some.server:9090/">>},
            typerefl:from_string(emqx_schema:url(), <<"http://some.server:9090">>)
        ),
        ?_assertEqual(
            {ok, <<"https://some.server:9090/">>},
            typerefl:from_string(emqx_schema:url(), <<"https://some.server:9090">>)
        ),
        ?_assertEqual(
            {ok, <<"https://some.server:9090/path?q=uery">>},
            typerefl:from_string(emqx_schema:url(), <<"https://some.server:9090/path?q=uery">>)
        ),
        ?_assertEqual(
            {error, {unsupported_scheme, <<"postgres">>}},
            typerefl:from_string(emqx_schema:url(), <<"postgres://some.server:9090">>)
        ),
        ?_assertEqual(
            {error, empty_host_not_allowed},
            typerefl:from_string(emqx_schema:url(), <<"">>)
        )
    ].

env_test_() ->
    Do = fun emqx_schema:naive_env_interpolation/1,
    [
        {"undefined", fun() -> ?assertEqual(undefined, Do(undefined)) end},
        {"full env abs path",
            with_env_fn(
                "MY_FILE",
                "/path/to/my/file",
                fun() -> ?assertEqual("/path/to/my/file", Do("$MY_FILE")) end
            )},
        {"full env relative path",
            with_env_fn(
                "MY_FILE",
                "path/to/my/file",
                fun() -> ?assertEqual("path/to/my/file", Do("${MY_FILE}")) end
            )},
        %% we can not test windows style file join though
        {"windows style",
            with_env_fn(
                "MY_FILE",
                "path\\to\\my\\file",
                fun() -> ?assertEqual("path\\to\\my\\file", Do("$MY_FILE")) end
            )},
        {"dir no {}",
            with_env_fn(
                "MY_DIR",
                "/mydir",
                fun() -> ?assertEqual("/mydir/foobar", Do(<<"$MY_DIR/foobar">>)) end
            )},
        {"dir with {}",
            with_env_fn(
                "MY_DIR",
                "/mydir",
                fun() -> ?assertEqual("/mydir/foobar", Do(<<"${MY_DIR}/foobar">>)) end
            )},
        %% a trailing / should not cause the sub path to become absolute
        {"env dir with trailing /",
            with_env_fn(
                "MY_DIR",
                "/mydir//",
                fun() -> ?assertEqual("/mydir/foobar", Do(<<"${MY_DIR}/foobar">>)) end
            )},
        {"string dir with doulbe /",
            with_env_fn(
                "MY_DIR",
                "/mydir/",
                fun() -> ?assertEqual("/mydir/foobar", Do(<<"${MY_DIR}//foobar">>)) end
            )},
        {"env not found",
            with_env_fn(
                "MY_DIR",
                "/mydir/",
                fun() -> ?assertEqual("${MY_DIR2}//foobar", Do(<<"${MY_DIR2}//foobar">>)) end
            )}
    ].

with_env_fn(Name, Value, F) ->
    fun() ->
        with_envs(F, [{Name, Value}])
    end.

with_envs(Fun, Envs) ->
    with_envs(Fun, [], Envs).

with_envs(Fun, Args, [{_Name, _Value} | _] = Envs) ->
    set_envs(Envs),
    try
        apply(Fun, Args)
    after
        unset_envs(Envs)
    end.

set_envs([{_Name, _Value} | _] = Envs) ->
    lists:map(fun({Name, Value}) -> os:putenv(Name, Value) end, Envs).

unset_envs([{_Name, _Value} | _] = Envs) ->
    lists:map(fun({Name, _}) -> os:unsetenv(Name) end, Envs).

timeout_types_test_() ->
    [
        ?_assertEqual(
            {ok, 4294967295},
            typerefl:from_string(emqx_schema:timeout_duration(), <<"4294967295ms">>)
        ),
        ?_assertEqual(
            {ok, 4294967295},
            typerefl:from_string(emqx_schema:timeout_duration_ms(), <<"4294967295ms">>)
        ),
        ?_assertEqual(
            {ok, 4294967},
            typerefl:from_string(emqx_schema:timeout_duration_s(), <<"4294967000ms">>)
        ),
        ?_assertEqual(
            {error, "timeout value too large (max: 4294967295 ms)"},
            typerefl:from_string(emqx_schema:timeout_duration(), <<"4294967296ms">>)
        ),
        ?_assertEqual(
            {error, "timeout value too large (max: 4294967295 ms)"},
            typerefl:from_string(emqx_schema:timeout_duration_ms(), <<"4294967296ms">>)
        ),
        ?_assertEqual(
            {error, "timeout value too large (max: 4294967 s)"},
            typerefl:from_string(emqx_schema:timeout_duration_s(), <<"4294967001ms">>)
        )
    ].

unicode_template_test() ->
    Sc = #{
        roots => [root],
        fields => #{root => [{template, #{type => emqx_schema:template()}}]}
    },
    HoconText = <<"root = {template = \"中文\"}"/utf8>>,
    {ok, Hocon} = hocon:binary(HoconText),
    ?assertEqual(
        #{<<"root">> => #{<<"template">> => <<"中文"/utf8>>}},
        hocon_tconf:check_plain(Sc, Hocon)
    ).

max_packet_size_test_() ->
    Sc = emqx_schema,
    Check = fun(Input) ->
        {ok, Hocon} = hocon:binary(Input),
        hocon_tconf:check_plain(Sc, Hocon, #{}, [mqtt])
    end,
    [
        {"one byte less than 256MB", fun() ->
            ?assertMatch(
                #{<<"mqtt">> := #{<<"max_packet_size">> := 268435455}},
                Check(<<"mqtt.max_packet_size = 256MB">>)
            )
        end},
        {"default value", fun() ->
            ?assertMatch(
                #{<<"mqtt">> := #{<<"max_packet_size">> := 1048576}},
                Check(<<"mqtt.max_packet_size = null">>)
            )
        end},
        {"1KB is 1024 bytes", fun() ->
            ?assertMatch(
                #{<<"mqtt">> := #{<<"max_packet_size">> := 1024}},
                Check(<<"mqtt.max_packet_size = 1KB">>)
            )
        end},
        {"257MB is not allowed", fun() ->
            ?assertThrow(
                {emqx_schema, [
                    #{reason := #{cause := max_mqtt_packet_size_too_large, maximum := 268435455}}
                ]},
                Check(<<"mqtt.max_packet_size = 257MB">>)
            )
        end},
        {"0 is not allowed", fun() ->
            ?assertThrow(
                {emqx_schema, [
                    #{reason := #{cause := max_mqtt_packet_size_too_small, minimum := 1}}
                ]},
                Check(<<"mqtt.max_packet_size = 0">>)
            )
        end}
    ].
