%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Tests for the shipped Erlang distribution TLS option file
%% (apps/emqx/etc/ssl_dist.conf) and for the OTP ssl behaviour that file
%% relies on. The handshake cases run over loopback, without an EMQX node.
-module(emqx_ssl_dist_conf_tests).

-include_lib("eunit/include/eunit.hrl").

-define(HANDSHAKE_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% The shipped option file
%%--------------------------------------------------------------------

%% OTP reads the file with erl_scan/erl_parse/erl_eval
%% (ssl_dist_sup:consult/1), so it must be exactly one Erlang expression.
conf_is_one_expression_test() ->
    ?assertMatch([{server, _}, {client, _}], consult_shipped_conf()).

conf_verifies_peers_test() ->
    Server = shipped(server),
    Client = shipped(client),
    ?assertEqual(verify_peer, proplists:get_value(verify, Server)),
    ?assertEqual(true, proplists:get_value(fail_if_no_peer_cert, Server)),
    ?assertEqual(verify_peer, proplists:get_value(verify, Client)),
    ?assertEqual(disable, proplists:get_value(server_name_indication, Client)).

%% The certificates shipped from EMQX 6.3 on carry an extended key usage, so a
%% server (serverAuth) certificate is rejected in the client role. The client
%% entry must name a clientAuth certificate.
conf_presents_a_client_certificate_test() ->
    Client = shipped(client),
    ?assertEqual("client-cert.pem", filename:basename(file_opt(Client, certfile))),
    ?assertEqual("client-key.pem", filename:basename(file_opt(Client, keyfile))),
    ?assertNotEqual(file_opt(Client, certfile), file_opt(Client, keyfile)).

%% The file must not turn peer verification off anywhere, not even in an entry
%% the behavioural tests above do not drive.
conf_does_not_disable_verification_test() ->
    ?assertEqual(nomatch, string:find(read_conf(), "verify_none")).

conf_names_the_certificates_it_expects_test() ->
    Opts = consult_shipped_conf(),
    Paths = [
        Path
     || Role <- [server, client],
        Key <- [certfile, keyfile, cacertfile],
        Path <- [file_opt(proplists:get_value(Role, Opts), Key)],
        is_list(Path)
    ],
    %% cert.pem, key.pem, cacert.pem, client-cert.pem and client-key.pem,
    %% with cacert.pem shared by both entries.
    ?assertEqual(
        ["cacert.pem", "cert.pem", "client-cert.pem", "client-key.pem", "key.pem"],
        lists:usort([filename:basename(Path) || Path <- Paths])
    ),
    %% EMQX ships no certificates any more (7.0), so the files themselves are
    %% the deployment's to provide; where the file looks for them is still its
    %% contract, because a package upgrade does not replace anything here.
    ?assertEqual(
        [],
        [
            Path
         || Path <- Paths,
            filename:dirname(Path) =/= filename:join(etc_dir(), "certs")
        ]
    ).

%%--------------------------------------------------------------------
%% The ssl behaviour the option file relies on, over loopback
%%--------------------------------------------------------------------

%% The strongest check: the option file as shipped completes a mutual TLS
%% handshake, including the host name check being disabled.
shipped_conf_handshakes_test() ->
    {ClientResult, ServerResult} = handshake(pki_opts(server), pki_opts(client)),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% The generated server certificate has a SAN for 127.0.0.1, so a node name
%% whose host part is a loopback address also works without disabling the host
%% name check.
shipped_conf_accepts_loopback_sni_test() ->
    {ClientResult, ServerResult} = handshake(
        pki_opts(server), with_sni("127.0.0.1", pki_opts(client))
    ),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% Any other node name needs the SAN to match, which is why the shipped file
%% disables the host name check.
shipped_conf_rejects_unknown_hostname_test() ->
    {ClientResult, _ServerResult} = handshake(
        pki_opts(server), with_sni("node1.emqx.io", pki_opts(client))
    ),
    ok = assert_alert_contains(ClientResult, "hostname_check_failed").

%% The `disable' line is what the test above relies on, and OTP honours it in
%% the distribution too, where `inet_tls_dist' passes the peer node's host part
%% as `server_name_indication': it passes it as a *default* (the third argument
%% of `inet_tcp_dist:merge_options/3'), and that function drops a default whose
%% key the options already set (lib/ssl/src/inet_tls_dist.erl:650,
%% lib/kernel/src/inet_tcp_dist.erl:226-268). A peer certificate without a SAN
%% for the host the client dials is accepted, which is what lets a cluster
%% whose node names the certificate does not carry connect; without the line the
%% same pair fails on the name.
shipped_conf_disables_hostname_check_test() ->
    #{cacertfile := Ca, certfile := Cert, keyfile := Key} =
        emqx_common_test_helpers:mock_server_certs(
            filename:dirname(test_cert("cacert.pem")), "not-the-dialed-host"
        ),
    %% fail_if_no_peer_cert=false: these cases are about the client's check of
    %% the server name, not about the server demanding a client certificate.
    Server = [
        {certfile, Cert},
        {keyfile, Key},
        {cacertfile, Ca},
        {verify, verify_peer},
        {fail_if_no_peer_cert, false}
    ],
    Client = [{cacertfile, Ca} | without(cacertfile, without_cert(pki_opts(client)))],
    {ClientResult, ServerResult} = handshake(Server, Client),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult),
    {ClientResultOn, _ServerResultOn} = handshake(
        Server, without(server_name_indication, Client)
    ),
    ok = assert_alert_contains(ClientResultOn, "hostname_check_failed").

%% The server asks for a certificate, so a peer without one cannot connect.
client_without_certificate_rejected_test() ->
    {_ClientResult, ServerResult} = handshake(pki_opts(server), without_cert(pki_opts(client))),
    ok = assert_alert_contains(ServerResult, "no_client_certificate_provided").

%% fail_if_no_peer_cert=false keeps the certificate chain check for peers that
%% do present a certificate, but lets a peer without one in.
client_certificate_optional_test() ->
    Server = with(fail_if_no_peer_cert, false, pki_opts(server)),
    {ClientResult, ServerResult} = handshake(Server, without_cert(pki_opts(client))),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% The client entry names a certificate of its own rather than reusing the
%% server's (`conf_presents_a_client_certificate_test' above). That matters
%% because a certificate can be restricted to one role: EMQX's own server
%% certificates carry the `serverAuth' extended key usage, and `ssl' rejects
%% them in the client role. The suite no longer generates such a pair - the
%% sets the suites generate for themselves are valid in both roles - so the
%% role restriction is not exercised here.

%% verify_peer without a CA is not a valid option combination at all.
missing_cacertfile_test() ->
    Server = without(cacertfile, pki_opts(server)),
    ?assertEqual(
        {error, {options, incompatible, [{verify, verify_peer}, {cacerts, undefined}]}},
        ssl:listen(0, Server)
    ).

%% A server certificate signed by a CA that the client does not trust is
%% rejected: this is what the new default buys, and it is the A/B pair below.
unrelated_ca_is_rejected_test() ->
    {ClientResult, _ServerResult} = handshake(rogue_server_opts(), pki_opts(client)),
    %% The certificate chain failure surfaces as this alert; the detailed
    %% {bad_cert, unknown_ca} reason is not part of the client's message.
    ?assertEqual(unknown_ca, assert_alert(ClientResult)).

%% Reproduces the previous default (server and client both verify_none): the
%% same rogue certificate was accepted, which is the issue this change fixes.
unverified_configuration_accepts_any_server_test() ->
    {ClientResult, ServerResult} = handshake(old_style_rogue_server_opts(), [{verify, verify_none}]),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

shipped(Role) ->
    proplists:get_value(Role, consult_shipped_conf()).

%% The shipped options with the certificate files it names replaced by the set
%% the suites generate for themselves (`emqx_common_test_helpers:test_cert/1').
%% The handshake tests need files on disk, and EMQX no longer ships an example
%% set (7.0): the file names the certificates, the deployment provides them.
%% Everything the tests are about - verify_peer, fail_if_no_peer_cert, the
%% client certificate and the disabled host name check - is unchanged.
pki_opts(Role) ->
    lists:foldl(
        fun(Key, Opts) ->
            case proplists:get_value(Key, Opts) of
                undefined -> Opts;
                Path -> with(Key, test_cert(filename:basename(Path)), Opts)
            end
        end,
        shipped(Role),
        [certfile, keyfile, cacertfile]
    ).

test_cert(Name) ->
    emqx_common_test_helpers:test_cert(Name).

consult_shipped_conf() ->
    Rendered = binary:replace(
        read_conf(),
        <<"{{ platform_etc_dir }}">>,
        list_to_binary(etc_dir()),
        [global]
    ),
    {ok, Tokens, _} = erl_scan:string(binary_to_list(Rendered)),
    {ok, Exprs} = erl_parse:parse_exprs(Tokens),
    {value, Opts, _} = erl_eval:exprs(Exprs, erl_eval:new_bindings()),
    Opts.

read_conf() ->
    {ok, Bin} = file:read_file(conf_path()),
    Bin.

conf_path() ->
    filename:join(etc_dir(), "ssl_dist.conf").

etc_dir() ->
    emqx_common_test_helpers:app_path(emqx, "etc").

file_opt(Opts, Key) ->
    proplists:get_value(Key, Opts).

with(Key, Value, Opts) ->
    lists:keyreplace(Key, 1, Opts, {Key, Value}).

without(Key, Opts) ->
    [Opt || {K, _} = Opt <- Opts, K =/= Key].

with_sni(Sni, Opts) ->
    with(server_name_indication, Sni, Opts).

without_cert(Opts) ->
    without(certfile, without(keyfile, Opts)).

%% A server certificate from a CA the shipped cacertfile knows nothing about.
rogue_server_opts() ->
    {RootDer, Key, PeerDer} = rogue_certs(),
    [
        {cert, PeerDer},
        {key, Key},
        {cacerts, [RootDer]},
        {verify, verify_peer},
        {fail_if_no_peer_cert, true}
    ].

old_style_rogue_server_opts() ->
    {_RootDer, Key, PeerDer} = rogue_certs(),
    [{cert, PeerDer}, {key, Key}, {verify, verify_none}].

rogue_certs() ->
    Root = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    {RootCert, _RootKey} = Root,
    {PeerCert, PeerKey} = emqx_cth_tls:gen_cert(#{key => ec, issuer => Root}),
    {der_cert(RootCert), der_key(PeerKey), der_cert(PeerCert)}.

der_cert({'Certificate', Der, _}) ->
    Der.

der_key({KeyType, Der, _}) ->
    {KeyType, Der}.

%% Both sockets of the pair are owned by a process of its own. An `ssl' socket
%% is in `{active, true}' unless the options say otherwise (ssl.erl:208), and an
%% active socket's owner is told about asynchronous events - when the peer
%% closes the transport, `ssl' delivers `{ssl_closed, Socket}'
%% (ssl_gen_statem.erl:1030-1061 and ssl_alert.erl:64). EUnit runs all tests of
%% an application in one process (a `{application, _, Modules}' group has no
%% `spawn', so eunit_proc:handle_group/2 runs it in place), and some tests -
%% emqx_router_syncer:batch_test/0 - assert on the exact content of that
%% mailbox, so a notification left behind here fails an unrelated test. Every
%% socket belongs to a worker and the caller waits for the worker's one report,
%% so nothing can reach its mailbox - not even when the worker overruns, which
%% is why it is killed and reaped rather than abandoned.
handshake(ServerOpts0, ClientOpts0) ->
    ServerOpts = [{active, false}, {reuseaddr, true} | with_default_versions(ServerOpts0)],
    ClientOpts = with_default_versions(ClientOpts0),
    Parent = self(),
    Ref = make_ref(),
    {Worker, MonRef} = spawn_monitor(fun() ->
        Parent ! {Ref, run_handshake(ServerOpts, ClientOpts)}
    end),
    receive
        {Ref, HandshakeResult} ->
            _ = erlang:demonitor(MonRef, [flush]),
            HandshakeResult;
        {'DOWN', MonRef, process, _, Reason} ->
            %% A worker that dies before reporting is a broken fixture, not a
            %% handshake result: fail here, where the reason is still readable.
            erlang:error({handshake_crashed, Reason})
    after ?HANDSHAKE_TIMEOUT ->
        %% An overrunning worker must not be left to report into the caller's
        %% mailbox later. Kill and reap it first, then drain: the 'DOWN' is the
        %% worker's last signal, so anything it sent before dying is already
        %% queued and this drain cannot race.
        exit(Worker, kill),
        receive
            {'DOWN', MonRef, process, _, _} -> ok
        end,
        receive
            {Ref, _} -> ok
        after 0 -> ok
        end,
        erlang:error({handshake_timeout, ?HANDSHAKE_TIMEOUT})
    end.

run_handshake(ServerOpts, ClientOpts) ->
    {ok, Listen} = ssl:listen(0, ServerOpts),
    {ok, {_, Port}} = ssl:sockname(Listen),
    Worker = self(),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        {ok, TSocket} = ssl:transport_accept(Listen),
        Worker ! {server_result, ssl:handshake(TSocket)}
    end),
    ClientResult = close_or_error(ssl:connect("127.0.0.1", Port, ClientOpts, ?HANDSHAKE_TIMEOUT)),
    ServerResult =
        receive
            {server_result, Result} -> close_or_error(Result);
            {'DOWN', MonRef, process, _, Reason} -> {server_crashed, Reason}
        after ?HANDSHAKE_TIMEOUT -> server_timeout
        end,
    _ = erlang:demonitor(MonRef, [flush]),
    _ = ssl:close(Listen),
    {ClientResult, ServerResult}.

%% OTP reads the option file as-is, but the distribution pins TLS 1.2 when the
%% file does not choose versions (inet_tls_dist:dist_defaults/1), so the tests
%% run at the version the distribution would negotiate.
with_default_versions(Opts) ->
    case proplists:is_defined(versions, Opts) of
        true -> Opts;
        false -> [{versions, ['tlsv1.2']} | Opts]
    end.

close_or_error({ok, Socket}) ->
    _ = ssl:close(Socket),
    ok;
close_or_error(Error) ->
    Error.

assert_alert_contains(Result, Text) ->
    Message = assert_alert_message(Result),
    ?assertNotEqual(nomatch, string:find(Message, Text)),
    ok.

assert_alert(Result) ->
    {error, {tls_alert, {Alert, _}}} = Result,
    Alert.

assert_alert_message(Result) ->
    case Result of
        {error, {tls_alert, {_Alert, Message}}} -> Message;
        _ -> erlang:error({expected_alert, Result})
    end.
