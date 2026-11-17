%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_niftls_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(ECHO_SIZE, 1024 * 1024).
-define(config(K, C), proplists:get_value(K, C)).

all() ->
    [F || {F, 1} <- ?MODULE:module_info(exports), lists:prefix("t_", atom_to_list(F))].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    Dir = ?config(priv_dir, Config),
    [
        {rsa, test_data({rsa, 2048, 65537})},
        {ecdsa, test_data({namedCurve, secp256r1})},
        {openssl_rsa, openssl_client_chain(Dir, "rsa", "-newkey rsa:2048")},
        {openssl_ec,
            openssl_client_chain(Dir, "ec", "-newkey ec -pkeyopt ec_paramgen_curve:prime256v1")}
        | Config
    ].

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "TLS 1.3 handshake with an OTP ssl client, then a 1 MB echo in both directions.".
t_tls13_echo(Config) ->
    #{server := Server, info := Info, client := Client} =
        run(server_opts(rsa, Config), client_opts(rsa, Config, [{versions, ['tlsv1.3']}]), echo),
    ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
    ?assertMatch(#{protocol := 'tlsv1.3', peercert := undefined, peer_certs := []}, Info),
    ?assertEqual(maps:get(selected_cipher_suite, Client), maps:get(selected_cipher_suite, Info)),
    ?assertMatch(#{closed := true}, Server).

-doc "TLS 1.2 handshake with ECDHE-RSA-AES256-GCM-SHA384, then an echo.".
t_tls12_echo(Config) ->
    ClientOpts = [{versions, ['tlsv1.2']}, {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384"]}],
    #{info := Info, client := Client} =
        run(server_opts(rsa, Config), client_opts(rsa, Config, ClientOpts), echo),
    ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
    ?assertMatch(
        #{
            protocol := 'tlsv1.2',
            selected_cipher_suite := #{key_exchange := ecdhe_rsa, cipher := aes_256_gcm}
        },
        Info
    ).

-doc "A server with an ECDSA certificate serves TLS 1.2 and TLS 1.3 clients.".
t_ecdsa_cert(Config) ->
    lists:foreach(
        fun(Version) ->
            #{info := Info, client := Client} =
                run(
                    server_opts(ecdsa, Config),
                    client_opts(ecdsa, Config, [{versions, [Version]}]),
                    echo
                ),
            ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
            ?assertMatch(#{protocol := Version}, Info)
        end,
        ['tlsv1.2', 'tlsv1.3']
    ).

-doc "Mutual TLS with RSA and ECDSA client certificates: the handshake info has the client certificate.".
t_mtls(Config) ->
    lists:foreach(
        fun({Kind, Version}) ->
            #{certfile := CertFile} = ?config(Kind, Config),
            [ClientCert] = read_certs(CertFile),
            ClientOpts = client_opts(rsa, Config, [
                {versions, [Version]} | client_cert(Kind, Config)
            ]),
            #{info := Info, client := Client} = run(
                mtls_server_opts(Kind, Config), ClientOpts, echo
            ),
            ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
            ?assertMatch(#{protocol := Version, peercert := ClientCert}, Info)
        end,
        [{K, V} || K <- [openssl_rsa, openssl_ec], V <- ['tlsv1.2', 'tlsv1.3']]
    ).

-doc "Mutual TLS with `fail_if_no_peer_cert`: a client without a certificate is rejected.".
t_mtls_no_client_cert(Config) ->
    #{server := Server, client := Client} =
        run(mtls_server_opts(openssl_ec, Config), client_opts(rsa, Config, []), echo),
    ?assertMatch(#{close := <<"peer sent no certificates">>}, Server),
    ?assertNotMatch(#{echoed := _}, Client).

-doc "With `verify_peer` and no `fail_if_no_peer_cert`, a client without a certificate is accepted.".
t_verify_peer_optional_cert(Config) ->
    ServerOpts = maps:put(fail_if_no_peer_cert, false, mtls_server_opts(openssl_ec, Config)),
    #{info := Info, client := Client} = run(ServerOpts, client_opts(rsa, Config, []), echo),
    ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
    ?assertMatch(#{peercert := undefined}, Info).

-doc "A client certificate from an unknown CA is rejected.".
t_mtls_unknown_ca(Config) ->
    %% The server trusts the CA of the EC client chain, the client presents the RSA chain.
    ClientOpts = client_opts(rsa, Config, client_cert(openssl_rsa, Config)),
    #{server := Server, client := Client} = run(
        mtls_server_opts(openssl_ec, Config), ClientOpts, echo
    ),
    ?assertMatch(#{close := <<"invalid peer certificate: UnknownIssuer">>}, Server),
    ?assertNotMatch(#{echoed := _}, Client).

-doc """
A known difference from OTP ssl: the Rustls verifier rejects the client chains that
`public_key:pkix_test_data/1` makes. The RSA public key has no NULL algorithm parameters, which
RFC 3279 requires. The ecdsa-with-SHA256 signature algorithm has NULL parameters, which RFC 5758
forbids. OTP ssl accepts both. OpenSSL encodes both correctly (see `t_mtls`).
""".
t_mtls_otp_test_chain(Config) ->
    lists:foreach(
        fun({Kind, Expected}) ->
            #{server_config := ServerConf} = ?config(Kind, Config),
            ServerOpts = maps:merge(server_opts(rsa, Config), #{
                verify => verify_peer,
                fail_if_no_peer_cert => true,
                cacerts => proplists:get_value(cacerts, ServerConf)
            }),
            ClientOpts = client_opts(rsa, Config, pkix_client_cert(Kind, Config)),
            #{server := Server, client := Client} = run(ServerOpts, ClientOpts, echo),
            #{close := <<"invalid peer certificate: ", Reason/binary>>} = Server,
            ?assertMatch({0, _}, binary:match(Reason, Expected)),
            ?assertNotMatch(#{echoed := _}, Client)
        end,
        [
            {rsa, <<"UnsupportedSignatureAlgorithmForPublicKey">>},
            {ecdsa, <<"UnsupportedSignatureAlgorithm">>}
        ]
    ).

-doc "A client that offers only TLS 1.2 fails against a TLS 1.3 only server and gets an alert.".
t_version_mismatch(Config) ->
    ServerOpts = maps:put(versions, ['tlsv1.3'], server_opts(rsa, Config)),
    #{server := Server, client := Client} =
        run(ServerOpts, client_opts(rsa, Config, [{versions, ['tlsv1.2']}]), echo),
    ?assertMatch(#{close := _}, Server),
    ?assertMatch(#{connect_error := {tls_alert, {protocol_version, _}}}, Client).

-doc "The handshake info has SNI and the negotiated ALPN protocol.".
t_sni_alpn(Config) ->
    ServerOpts = maps:put(alpn_preferred_protocols, [<<"mqtt">>], server_opts(rsa, Config)),
    ClientOpts = client_opts(rsa, Config, [{alpn_advertised_protocols, [<<"x">>, <<"mqtt">>]}]),
    #{info := Info} = run(ServerOpts, ClientOpts, echo),
    ?assertMatch(#{sni_hostname := "localhost", alpn := <<"mqtt">>}, Info).

-doc "The server cipher suite list limits the negotiated suite.".
t_server_ciphers(Config) ->
    ServerOpts = maps:merge(server_opts(rsa, Config), #{
        ciphers => ["TLS_CHACHA20_POLY1305_SHA256", "ECDHE-RSA-AES128-GCM-SHA256"],
        supported_groups => [secp256r1]
    }),
    #{info := Info13} = run(ServerOpts, client_opts(rsa, Config, [{versions, ['tlsv1.3']}]), echo),
    ?assertMatch(#{selected_cipher_suite := #{cipher := chacha20_poly1305}}, Info13),
    #{info := Info12} = run(ServerOpts, client_opts(rsa, Config, [{versions, ['tlsv1.2']}]), echo),
    ?assertMatch(#{selected_cipher_suite := #{cipher := aes_128_gcm}}, Info12).

-doc "`close/1` returns a close_notify. The client then sees the connection close.".
t_server_close(Config) ->
    #{server := Server, client := Client} =
        run(server_opts(rsa, Config), client_opts(rsa, Config, []), server_close),
    ?assertMatch(#{sent_close := true}, Server),
    ?assertMatch(#{recv := {error, closed}}, Client).

-doc "Bytes that are not TLS give `close` with an alert to send.".
t_garbage_input(Config) ->
    {ok, Conf} = emqx_niftls:config(default_server_opts(Config)),
    Tls = emqx_niftls:new(Conf),
    Garbage = <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>,
    ?assertMatch(
        {close, Out, Reason} when is_binary(Reason) andalso Out =/= <<>>,
        emqx_niftls:feed(Tls, Garbage)
    ).

-doc "Certificate, key and CA from PEM files, with an encrypted private key.".
t_pem_files(Config) ->
    Dir = ?config(priv_dir, Config),
    #{server_config := ServerConf} = ?config(rsa, Config),
    {'RSAPrivateKey', KeyDer} = proplists:get_value(key, ServerConf),
    Key = public_key:der_decode('RSAPrivateKey', KeyDer),
    Password = "secret",
    CipherInfo = {"AES-128-CBC", crypto:strong_rand_bytes(16)},
    KeyEntry = public_key:pem_entry_encode('RSAPrivateKey', Key, {CipherInfo, Password}),
    CertFile = write_pem(Dir, "cert.pem", [
        {'Certificate', C, not_encrypted}
     || C <- chain(ServerConf)
    ]),
    KeyFile = write_pem(Dir, "key.pem", [KeyEntry]),
    CAFile = write_pem(Dir, "ca.pem", [
        {'Certificate', C, not_encrypted}
     || C <- proplists:get_value(cacerts, ServerConf)
    ]),
    Opts = #{certfile => CertFile, keyfile => KeyFile, password => Password, cacertfile => CAFile},
    #{client := Client} = run(Opts, client_opts(rsa, Config, []), echo),
    ?assertMatch(#{echoed := ?ECHO_SIZE}, Client),
    ?assertMatch(
        {error, #{reason := bad_private_key_password}},
        emqx_niftls:config(Opts#{password => "wrong"})
    ),
    %% OpenSSL writes PKCS#8 keys.
    #{certfile := RsaCert, keyfile := RsaKey} = ?config(openssl_rsa, Config),
    #{certfile := EcCert, keyfile := EcKey} = ?config(openssl_ec, Config),
    ?assertMatch({ok, _}, emqx_niftls:config(#{certfile => RsaCert, keyfile => RsaKey})),
    ?assertMatch({ok, _}, emqx_niftls:config(#{certfile => EcCert, keyfile => EcKey})),
    ?assertMatch(
        {error, #{reason := key_does_not_match_certificate}},
        emqx_niftls:config(#{certfile => EcCert, keyfile => RsaKey})
    ).

-doc "Options that Rustls cannot honor fail with an error that names the option.".
t_config_errors(Config) ->
    Base = default_server_opts(Config),
    ?assertMatch(
        {error, #{reason := unsupported_option, option := user_lookup_fun}},
        emqx_niftls:config(Base#{user_lookup_fun => {fun(_, _, _) -> error end, undefined}})
    ),
    ?assertMatch(
        {error, #{reason := unsupported_option, option := crl_check}},
        emqx_niftls:config(Base#{crl_check => peer})
    ),
    ?assertMatch(
        {error, #{reason := unsupported_tls_version, version := 'tlsv1.1'}},
        emqx_niftls:config(Base#{versions => ['tlsv1.2', 'tlsv1.1']})
    ),
    ?assertMatch(
        {error, #{reason := unsupported_cipher, cipher := "AES256-SHA"}},
        emqx_niftls:config(Base#{ciphers => ["ECDHE-RSA-AES256-GCM-SHA384", "AES256-SHA"]})
    ),
    ?assertMatch(
        {error, #{reason := unsupported_group, group := x448}},
        emqx_niftls:config(Base#{supported_groups => [x25519, x448]})
    ),
    ?assertMatch(
        {error, #{reason := rustls_config_error}},
        emqx_niftls:config(Base#{verify => verify_peer, cacerts => []})
    ),
    %% A key that does not match the certificate.
    #{server_config := EcConf} = ?config(ecdsa, Config),
    ?assertMatch(
        {error, #{reason := key_does_not_match_certificate}},
        emqx_niftls:config(Base#{key => proplists:get_value(key, EcConf)})
    ),
    #{server_config := OtherRsaConf} = test_data({rsa, 2048, 65537}),
    ?assertMatch(
        {error, #{reason := key_does_not_match_certificate}},
        emqx_niftls:config(Base#{key => proplists:get_value(key, OtherRsaConf)})
    ),
    %% Options that have no effect are accepted.
    ?assertMatch({ok, _}, emqx_niftls:config(Base#{handshake_timeout => 1000, depth => 10})).

-doc "Configurations and connections are freed when no process refers to them.".
t_no_leak(Config) ->
    Opts = default_server_opts(Config),
    Before = emqx_nif:info(),
    {Pid, MRef} = spawn_monitor(fun() ->
        {ok, Conf} = emqx_niftls:config(Opts),
        Tlss = [emqx_niftls:new(Conf) || _ <- lists:seq(1, 100)],
        #{tls_conns := N} = emqx_nif:info(),
        true = N >= 100,
        exit({done, length(Tlss)})
    end),
    receive
        {'DOWN', MRef, process, Pid, Reason} -> ?assertEqual({done, 100}, Reason)
    end,
    wait_until(fun() -> emqx_nif:info() =:= Before end, 50).

%%--------------------------------------------------------------------
%% Helpers: certificates and options
%%--------------------------------------------------------------------

test_data(KeySpec) ->
    SAN = #'Extension'{
        extnID = ?'id-ce-subjectAltName',
        extnValue = [{dNSName, "localhost"}],
        critical = false
    },
    Opts = [{key, KeySpec}, {digest, sha256}],
    Chain = #{root => Opts, intermediates => [Opts], peer => Opts},
    %% In the result, `cacerts' of each side has the root of the peer chain, and the root and
    %% the intermediate of its own chain.
    public_key:pkix_test_data(#{
        server_chain => Chain#{peer => [{extensions, [SAN]} | Opts]},
        client_chain => Chain
    }).

server_opts(Kind, Config) ->
    #{server_config := ServerConf} = ?config(Kind, Config),
    #{
        cert => chain(ServerConf),
        key => proplists:get_value(key, ServerConf)
    }.

default_server_opts(Config) ->
    server_opts(rsa, Config).

%% Request and verify a client certificate from the OpenSSL chain `Kind'.
mtls_server_opts(Kind, Config) ->
    #{cacertfile := CAFile} = ?config(Kind, Config),
    maps:merge(server_opts(rsa, Config), #{
        verify => verify_peer,
        fail_if_no_peer_cert => true,
        cacertfile => CAFile
    }).

client_opts(Kind, Config, Extra) ->
    #{client_config := ClientConf} = ?config(Kind, Config),
    Extra ++
        [
            {verify, verify_peer},
            {cacerts, proplists:get_value(cacerts, ClientConf)},
            {server_name_indication, "localhost"},
            {mode, binary},
            {active, false}
        ].

client_cert(Kind, Config) ->
    #{certfile := CertFile, keyfile := KeyFile} = ?config(Kind, Config),
    [{certfile, CertFile}, {keyfile, KeyFile}].

pkix_client_cert(Kind, Config) ->
    #{client_config := ClientConf} = ?config(Kind, Config),
    [{cert, chain(ClientConf)}, {key, proplists:get_value(key, ClientConf)}].

%% A CA and a client certificate, made with the OpenSSL command line tool.
openssl_client_chain(Dir0, Name, NewKey) ->
    Dir = filename:join(Dir0, Name),
    ok = filelib:ensure_path(Dir),
    ExtFile = filename:join(Dir, "client.ext"),
    ok = file:write_file(ExtFile, <<"""
    basicConstraints = critical, CA:FALSE
    keyUsage = critical, digitalSignature
    extendedKeyUsage = clientAuth
    """>>),
    Cmds = [
        "openssl req -x509 -nodes -days 2 " ++ NewKey ++
            " -keyout ca.key -out ca.pem -subj /CN=niftls-ca-" ++ Name ++
            " -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign",
        "openssl req -nodes " ++ NewKey ++ " -keyout client.key -out client.csr -subj /CN=client-" ++
            Name,
        "openssl x509 -req -days 2 -in client.csr -CA ca.pem -CAkey ca.key -set_serial 2"
        " -extfile client.ext -out client.pem"
    ],
    lists:foreach(
        fun(Cmd) ->
            {Status, Output} = run_cmd(Cmd, Dir),
            Status =:= 0 orelse ct:fail({openssl_failed, Cmd, Output})
        end,
        Cmds
    ),
    #{
        cacertfile => filename:join(Dir, "ca.pem"),
        certfile => filename:join(Dir, "client.pem"),
        keyfile => filename:join(Dir, "client.key")
    }.

run_cmd(Cmd, Dir) ->
    Port = open_port({spawn, Cmd}, [{cd, Dir}, exit_status, stderr_to_stdout, binary]),
    collect_port(Port, []).

collect_port(Port, Acc) ->
    receive
        {Port, {data, Data}} -> collect_port(Port, [Data | Acc]);
        {Port, {exit_status, Status}} -> {Status, iolist_to_binary(lists:reverse(Acc))}
    after 30000 -> ct:fail(openssl_timeout)
    end.

read_certs(File) ->
    {ok, Pem} = file:read_file(File),
    [Der || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(Pem)].

%% The leaf certificate and the intermediate CAs, without the root.
chain(SideConf) ->
    Leaf = proplists:get_value(cert, SideConf),
    CAs = proplists:get_value(cacerts, SideConf),
    [Leaf | intermediates(Leaf, CAs)].

intermediates(Cert, CAs) ->
    Issuer = issuer(Cert),
    case [CA || CA <- CAs, subject(CA) =:= Issuer, issuer(CA) =/= subject(CA)] of
        [Inter | _] -> [Inter | intermediates(Inter, CAs)];
        [] -> []
    end.

subject(Der) ->
    #'OTPCertificate'{tbsCertificate = TBS} = public_key:pkix_decode_cert(Der, otp),
    TBS#'OTPTBSCertificate'.subject.

issuer(Der) ->
    #'OTPCertificate'{tbsCertificate = TBS} = public_key:pkix_decode_cert(Der, otp),
    TBS#'OTPTBSCertificate'.issuer.

write_pem(Dir, Name, Entries) ->
    File = filename:join(Dir, Name),
    ok = file:write_file(File, public_key:pem_encode(Entries)),
    File.

%%--------------------------------------------------------------------
%% Helpers: a TCP server that uses the NIF, and an OTP ssl client
%%--------------------------------------------------------------------

%% Run one connection. Mode `echo': the server sends back all data. Mode `server_close': the
%% server sends close_notify after the first data and closes.
run(ServerOpts, ClientOpts, Mode) ->
    {ok, Conf} = emqx_niftls:config(ServerOpts),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(LSock),
    Parent = self(),
    {Pid, MRef} = spawn_monitor(fun() ->
        Parent ! {client, self(), client(Port, ClientOpts, Mode)}
    end),
    {ok, Sock} = gen_tcp:accept(LSock, 5000),
    ok = gen_tcp:close(LSock),
    Server = serve(Sock, emqx_niftls:new(Conf), Mode, #{}),
    Client =
        receive
            {client, Pid, Result} -> Result;
            {'DOWN', MRef, process, Pid, Reason} -> error({client_down, Reason})
        after 10000 -> error(client_timeout)
        end,
    #{server => Server, client => Client, info => maps:get(info, Server, undefined)}.

serve(Sock, Tls, Mode, St) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Bin} ->
            case emqx_niftls:feed(Tls, Bin) of
                {ok, Out, App, Tls1} ->
                    ok = send(Sock, Out),
                    handle_app(Sock, Tls1, Mode, App, St);
                {handshake_done, Out, App, Info, Tls1} ->
                    ok = send(Sock, Out),
                    handle_app(Sock, Tls1, Mode, App, St#{info => Info});
                {close, Out, Reason} ->
                    _ = send(Sock, Out),
                    ok = gen_tcp:close(Sock),
                    St#{close => Reason};
                {error, Reason} ->
                    ok = gen_tcp:close(Sock),
                    St#{error => Reason}
            end;
        {error, closed} ->
            St#{closed => true};
        {error, Reason} ->
            ok = gen_tcp:close(Sock),
            St#{recv_error => Reason}
    end.

handle_app(Sock, Tls, _Mode, <<>>, St) ->
    serve(Sock, Tls, _Mode, St);
handle_app(Sock, Tls, echo, App, St) ->
    {ok, Out} = emqx_niftls:encrypt(Tls, App),
    ok = send(Sock, Out),
    serve(Sock, Tls, echo, St);
handle_app(Sock, Tls, server_close, _App, St) ->
    ok = send(Sock, emqx_niftls:close(Tls)),
    ok = gen_tcp:close(Sock),
    St#{sent_close => true}.

send(Sock, Data) ->
    case iolist_size(Data) of
        0 -> ok;
        _ -> gen_tcp:send(Sock, Data)
    end.

client(Port, Opts, Mode) ->
    case ssl:connect({127, 0, 0, 1}, Port, Opts, 5000) of
        {ok, S} ->
            {ok, [{selected_cipher_suite, Suite}]} =
                ssl:connection_information(S, [selected_cipher_suite]),
            Result = client_run(S, Mode),
            _ = ssl:close(S),
            Result#{selected_cipher_suite => Suite};
        {error, Reason} ->
            #{connect_error => Reason}
    end.

client_run(S, echo) ->
    Data = crypto:strong_rand_bytes(?ECHO_SIZE),
    case ssl:send(S, Data) of
        ok ->
            case recv_n(S, ?ECHO_SIZE, []) of
                {ok, Data} -> #{echoed => ?ECHO_SIZE};
                {ok, _Other} -> #{echo_mismatch => true};
                {error, Reason} -> #{recv => {error, Reason}}
            end;
        {error, Reason} ->
            #{send => {error, Reason}}
    end;
client_run(S, server_close) ->
    ok = ssl:send(S, <<"bye">>),
    #{recv => ssl:recv(S, 0, 5000)}.

recv_n(_S, 0, Acc) ->
    {ok, iolist_to_binary(lists:reverse(Acc))};
recv_n(S, N, Acc) ->
    case ssl:recv(S, 0, 5000) of
        {ok, Bin} -> recv_n(S, N - byte_size(Bin), [Bin | Acc]);
        {error, _} = Error -> Error
    end.

wait_until(Fun, 0) ->
    ?assert(Fun());
wait_until(Fun, N) ->
    erlang:garbage_collect(),
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_until(Fun, N - 1)
    end.
