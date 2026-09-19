%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Table driven tests for emqx_machine:check_dist_tls_verify/2, the check that
%% reports a distribution TLS configuration which does not verify peers, and for
%% emqx_machine:merge_dist_tls_opts/2, which builds the options the VM applies
%% from the option file and the -ssl_dist_opt arguments.
-module(emqx_machine_dist_tls_tests).

-include_lib("eunit/include/eunit.hrl").

-define(GOOD_SERVER, [
    {certfile, "cert.pem"},
    {keyfile, "key.pem"},
    {cacertfile, "cacert.pem"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true}
]).

-define(GOOD_CLIENT, [
    {certfile, "client-cert.pem"},
    {keyfile, "client-key.pem"},
    {cacertfile, "cacert.pem"},
    {verify, verify_peer},
    {server_name_indication, disable}
]).

non_tls_distribution_is_not_checked_test() ->
    Opts = [{server, [{verify, verify_none}]}, {client, [{verify, verify_none}]}],
    ?assertEqual(ok, emqx_machine:check_dist_tls_verify("inet_tcp", Opts)),
    ?assertEqual(ok, emqx_machine:check_dist_tls_verify("inet6_tcp", Opts)).

shipped_configuration_is_ok_test() ->
    ?assertEqual(
        ok,
        emqx_machine:check_dist_tls_verify("inet_tls", opts(?GOOD_SERVER, ?GOOD_CLIENT))
    ).

check_dist_tls_verify_test_() ->
    Cases = [
        {"both sides verify_none", opts([{verify, verify_none}], [{verify, verify_none}]),
            {warn, [{server, verify_none}, {client, verify_none}]}},
        {"server verify_none, client verifies", opts([{verify, verify_none}], ?GOOD_CLIENT),
            {warn, [{server, verify_none}]}},
        {"server verifies but does not require a client certificate",
            %% Overriding `fail_if_no_peer_cert' means writing it after the value
            %% in ?GOOD_SERVER: the last occurrence is the one OTP applies.
            opts(?GOOD_SERVER ++ [{fail_if_no_peer_cert, false}], ?GOOD_CLIENT),
            {warn, [{server, fail_if_no_peer_cert_false}]}},
        {"empty option file", [], {warn, [{server, verify_none}]}},
        %% OTP folds a key that appears twice last-wins, so the second value is
        %% the one the VM applies.
        {"duplicate verify, the last one wins",
            opts(
                [{verify, verify_peer}, {verify, verify_none}],
                [{verify, verify_peer}, {verify, verify_none}]
            ),
            {warn, [{server, verify_none}, {client, verify_none}]}},
        %% A certificate, a key and a CA are `ssl's business: it reports them as
        %% an option error when it sets up the listener or the connection, so
        %% none of these entries is rejected here.
        {"client verifies without a client certificate",
            opts(?GOOD_SERVER, [{verify, verify_peer}, {cacertfile, "cacert.pem"}]), ok},
        {"client verifies without a CA",
            opts(?GOOD_SERVER, [{verify, verify_peer}, {certfile, "c.pem"}, {keyfile, "k.pem"}]),
            ok},
        {"server verifies without a CA",
            opts([{verify, verify_peer}, {fail_if_no_peer_cert, true}], ?GOOD_CLIENT), ok},
        {"client names no identity and no CA", opts(?GOOD_SERVER, [{verify, verify_peer}]), ok}
    ],
    [
        {Name, fun() ->
            ?assertEqual(Expected, emqx_machine:check_dist_tls_verify("inet_tls", Opts))
        end}
     || {Name, Opts, Expected} <- Cases
    ].

%% OTP defaults the client side to verify_peer, so an option file that omits
%% 'verify' in the client entry and names a CA and a certificate is accepted.
client_verify_defaults_to_verify_peer_test() ->
    Client = [O || {K, _} = O <- ?GOOD_CLIENT, K =/= verify],
    ?assertEqual(ok, emqx_machine:check_dist_tls_verify("inet_tls", opts(?GOOD_SERVER, Client))).

%% OTP folds the -ssl_dist_opt arguments and the option file into one list,
%% command line first (`inet_tls_dist:get_ssl_options/1'), so the option file is
%% the last occurrence and wins a key both set.
merge_dist_tls_opts_puts_the_command_line_first_test() ->
    ?assertEqual(
        [
            {server, []},
            {client, [{verify, verify_none}, {verify, verify_peer}]}
        ],
        emqx_machine:merge_dist_tls_opts(
            [{client, [{verify, verify_peer}]}],
            ["client_verify", "verify_none"]
        )
    ).

%% A -ssl_dist_opt argument is named `<role>_<key>': an argument named for the
%% other role is skipped, and a trailing argument without a value is ignored.
merge_dist_tls_opts_keeps_the_role_prefix_test() ->
    Opts = emqx_machine:merge_dist_tls_opts(
        [],
        ["client_verify", "verify_peer", "server_fail_if_no_peer_cert", "true", "dangling"]
    ),
    ?assertEqual([{verify, verify_peer}], proplists:get_value(client, Opts)),
    ?assertEqual([{fail_if_no_peer_cert, true}], proplists:get_value(server, Opts)).

%% The `-ssl_dist_opt' arguments are part of the options the VM applies, so a
%% 'verify_none' that only the command line sets must still be reported.
command_line_options_test_() ->
    ClientNoVerify = [O || {K, _} = O <- ?GOOD_CLIENT, K =/= verify],
    Cases = [
        {"client verify comes only from the command line", opts(?GOOD_SERVER, ClientNoVerify),
            ["client_verify", "verify_none"], {warn, [{client, verify_none}]}},
        {"server verify comes only from the command line",
            opts([{cacertfile, "cacert.pem"}], ?GOOD_CLIENT),
            ["server_verify", "verify_peer", "server_fail_if_no_peer_cert", "true"], ok},
        {"the option file overrides the command line", opts(?GOOD_SERVER, ?GOOD_CLIENT),
            ["client_verify", "verify_none"], ok},
        {"a command-line verify_none for one role does not change the other",
            opts([{cacertfile, "cacert.pem"}], ?GOOD_CLIENT), ["client_verify", "verify_none"],
            {warn, [{server, verify_none}]}}
    ],
    [
        {Name, fun() ->
            Opts = emqx_machine:merge_dist_tls_opts(TableOpts, CmdLineArgs),
            ?assertEqual(Expected, emqx_machine:check_dist_tls_verify("inet_tls", Opts))
        end}
     || {Name, TableOpts, CmdLineArgs, Expected} <- Cases
    ].

%% An option file that was supplied but is empty must still be judged: OTP
%% creates the `ssl_dist_opts' table for it, so `{ok, []}' is not the same as
%% having no option file at all.
dist_tls_opts_separates_an_empty_option_file_from_none_test() ->
    ?assertEqual(error, emqx_machine:dist_tls_opts(error, [])),
    ?assertEqual({ok, opts([], [])}, emqx_machine:dist_tls_opts({ok, []}, [])),
    ?assertEqual(
        {ok, opts([{verify, verify_none}], [])},
        emqx_machine:dist_tls_opts(error, ["server_verify", "verify_none"])
    ).

%% The two checks together: an empty option file leaves the server on OTP's
%% verify_none default, so it must be reported.
empty_option_file_is_reported_test() ->
    {ok, Opts} = emqx_machine:dist_tls_opts({ok, []}, []),
    ?assertEqual(
        {warn, [{server, verify_none}]},
        emqx_machine:check_dist_tls_verify("inet_tls", Opts)
    ).

opts(Server, Client) ->
    [{server, Server}, {client, Client}].
