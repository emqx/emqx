%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
                unknown := <<"gc_after_handshake">>
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
    Host = fun(Str) ->
        case Ip(Str) of
            {ok, {_, _} = Res} ->
                %% assert
                {ok, Res} = emqx_schema:to_host_port(Str);
            _ ->
                emqx_schema:to_host_port(Str)
        end
    end,
    [
        ?_assertEqual({ok, 80}, Ip("80")),
        ?_assertEqual({error, bad_host_port}, Host("80")),
        ?_assertEqual({ok, 80}, Ip(":80")),
        ?_assertEqual({error, bad_host_port}, Host(":80")),
        ?_assertEqual({error, bad_ip_port}, Ip("localhost:80")),
        ?_assertEqual({ok, {"localhost", 80}}, Host("localhost:80")),
        ?_assertEqual({ok, {"example.com", 80}}, Host("example.com:80")),
        ?_assertEqual({ok, {{127, 0, 0, 1}, 80}}, Ip("127.0.0.1:80")),
        ?_assertEqual({error, bad_ip_port}, Ip("$:1900")),
        ?_assertEqual({error, bad_hostname}, Host("$:1900")),
        ?_assertMatch({ok, {_, 1883}}, Ip("[::1]:1883")),
        ?_assertMatch({ok, {_, 1883}}, Ip("::1:1883")),
        ?_assertMatch({ok, {_, 1883}}, Ip(":::1883"))
    ].
