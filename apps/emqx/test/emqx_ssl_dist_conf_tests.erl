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

%% bin/emqx:maybe_check_dist_tls_verify/1 refuses to start (under a non-legacy
%% security profile) when this string is present in a line that is not a comment.
conf_does_not_disable_verification_test() ->
    ?assertEqual(nomatch, string:find(read_conf(), "verify_none")).

conf_referenced_certificates_exist_test() ->
    Opts = consult_shipped_conf(),
    Paths = lists:usort([
        Path
     || Role <- [server, client],
        Key <- [certfile, keyfile, cacertfile],
        Path <- [file_opt(proplists:get_value(Role, Opts), Key)],
        is_list(Path)
    ]),
    %% cert.pem, key.pem, cacert.pem, client-cert.pem and client-key.pem,
    %% with cacert.pem shared by both entries.
    ?assertEqual(5, length(Paths)),
    lists:foreach(
        fun(Path) -> ?assertMatch({ok, _}, file:read_file_info(Path)) end,
        Paths
    ).

%%--------------------------------------------------------------------
%% The ssl behaviour the option file relies on, over loopback
%%--------------------------------------------------------------------

%% The strongest check: the option file as shipped completes a mutual TLS
%% handshake, including the host name check being disabled.
shipped_conf_handshakes_test() ->
    {ClientResult, ServerResult} = handshake(shipped(server), shipped(client)),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% The shipped certificate has a SAN for 127.0.0.1, so a node name whose host
%% part is a loopback address also works without disabling the host name check.
shipped_conf_accepts_loopback_sni_test() ->
    {ClientResult, ServerResult} = handshake(
        shipped(server), with_sni("127.0.0.1", shipped(client))
    ),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% Any other node name needs the SAN to match, which is why the shipped file
%% disables the host name check.
shipped_conf_rejects_unknown_hostname_test() ->
    {ClientResult, _ServerResult} = handshake(
        shipped(server), with_sni("node1.emqx.io", shipped(client))
    ),
    ok = assert_alert_contains(ClientResult, "hostname_check_failed").

%% The server asks for a certificate, so a peer without one cannot connect.
client_without_certificate_rejected_test() ->
    {_ClientResult, ServerResult} = handshake(shipped(server), without_cert(shipped(client))),
    ok = assert_alert_contains(ServerResult, "no_client_certificate_provided").

%% fail_if_no_peer_cert=false keeps the certificate chain check for peers that
%% do present a certificate, but lets a peer without one in.
client_certificate_optional_test() ->
    Server = with(fail_if_no_peer_cert, false, shipped(server)),
    {ClientResult, ServerResult} = handshake(Server, without_cert(shipped(client))),
    ?assertEqual(ok, ClientResult),
    ?assertEqual(ok, ServerResult).

%% cert.pem is a serverAuth certificate: presenting it as a client certificate
%% is rejected, hence the client entry names client-cert.pem.
server_certificate_is_not_a_client_certificate_test() ->
    Client = [
        {certfile, cert_path("cert.pem")},
        {keyfile, cert_path("key.pem")}
        | without(certfile, without(keyfile, shipped(client)))
    ],
    {_ClientResult, ServerResult} = handshake(shipped(server), Client),
    ok = assert_alert_contains(ServerResult, "invalid_ext_keyusage").

%% verify_peer without a CA is not a valid option combination at all.
missing_cacertfile_test() ->
    Server = without(cacertfile, shipped(server)),
    ?assertEqual(
        {error, {options, incompatible, [{verify, verify_peer}, {cacerts, undefined}]}},
        ssl:listen(0, Server)
    ).

%% A server certificate signed by a CA that the client does not trust is
%% rejected: this is what the new default buys, and it is the A/B pair below.
unrelated_ca_is_rejected_test() ->
    {ClientResult, _ServerResult} = handshake(rogue_server_opts(), shipped(client)),
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

cert_path(Name) ->
    filename:join([etc_dir(), "certs", Name]).

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

handshake(ServerOpts0, ClientOpts0) ->
    ServerOpts = [{active, false}, {reuseaddr, true} | with_default_versions(ServerOpts0)],
    ClientOpts = with_default_versions(ClientOpts0),
    {ok, Listen} = ssl:listen(0, ServerOpts),
    {ok, {_, Port}} = ssl:sockname(Listen),
    Parent = self(),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        {ok, TSocket} = ssl:transport_accept(Listen),
        Parent ! {server_result, ssl:handshake(TSocket)}
    end),
    ClientResult = close_or_error(ssl:connect("127.0.0.1", Port, ClientOpts, ?HANDSHAKE_TIMEOUT)),
    ServerResult =
        receive
            {server_result, Result} -> close_or_error(Result);
            {'DOWN', MonRef, process, _, Reason} -> {server_crashed, Reason}
        after ?HANDSHAKE_TIMEOUT -> server_timeout
        end,
    %% Do not leave the monitor's 'DOWN' message behind: EUnit runs all tests of
    %% an application in one process, and some tests (for example
    %% emqx_router_syncer:batch_test/0) assert on the exact mailbox content.
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
