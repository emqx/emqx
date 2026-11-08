%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dist_tls_check).

-export([
    maybe_warn_dist_tls_verify/0, check_dist_tls_verify/2, merge_dist_tls_opts/2, dist_tls_opts/2
]).

-include_lib("emqx/include/logger.hrl").

%% @doc Check whether the Erlang distribution over TLS verifies peer
%% certificates. `Opts' is a proplist with a `server' and a `client' entry
%% holding the effective options of each side: the `-ssl_dist_optfile' content
%% with the `-ssl_dist_opt' arguments merged into it, as OTP applies them
%% (`merge_dist_tls_opts/2').
%%
%% Returns `ok', or `{warn, Problems}' when peer verification is not enforced.
%% A distribution over plain TCP is not checked.
%%
%% Only the options that decide whether a peer is verified are judged: a
%% `verify' that is not `verify_peer', and a server entry that does not require
%% the peer's certificate. Whether a certificate, a key or a CA is present and
%% usable is left to `ssl', which reports those as an option error when it sets
%% up the listener or the connection.
-spec check_dist_tls_verify(string(), proplists:proplist()) -> ok | {warn, [term()]}.
check_dist_tls_verify(ProtoDist, Opts) ->
    case is_dist_over_tls(ProtoDist) of
        false ->
            ok;
        true ->
            Problems =
                check_server_opts(proplists:get_value(server, Opts, [])) ++
                    check_client_opts(proplists:get_value(client, Opts, [])),
            case Problems of
                [] -> ok;
                _ -> {warn, Problems}
            end
    end.

%% The optfile is consulted by OTP while the VM starts, before any EMQX
%% application is up, so this runs after the distribution listener exists and
%% cannot prevent it from being created. It reports what it finds and lets the
%% node boot: the option file EMQX ships verifies peers, and a deployment that
%% deliberately turns that off is told so, not stopped. It judges the options
%% the VM actually applies, so unlike a text match on the file it also covers a
%% 'verify' that was computed at read time, an entry that omits 'verify' and
%% falls back to an OTP default, and a 'verify' that only a '-ssl_dist_opt'
%% argument sets.
maybe_warn_dist_tls_verify() ->
    ProtoDist = os:getenv("EKKA_PROTO_DIST_MOD", "inet_tcp"),
    case is_dist_over_tls(ProtoDist) of
        false ->
            ok;
        true ->
            case dist_tls_opts() of
                {ok, Opts} ->
                    case check_dist_tls_verify(ProtoDist, Opts) of
                        ok ->
                            ok;
                        {warn, Problems} ->
                            dist_tls_unverified(Problems)
                    end;
                error ->
                    %% No option file and no '-ssl_dist_opt' argument: nothing
                    %% was handed to OTP that this check could judge.
                    ok
            end
    end.

dist_tls_opts() ->
    dist_tls_opts(dist_tls_table_opts(), dist_tls_cmd_line_args()).

%% @doc `TableOpts' is `{ok, Opts}' when OTP was given an option file - the file
%% may hold an empty option list - and `error' when it was not given one, so an
%% option file that was supplied but is empty is still judged. Only when there is
%% no option file and no `-ssl_dist_opt' argument either is there nothing to
%% judge.
-spec dist_tls_opts({ok, proplists:proplist()} | error, [string()]) ->
    {ok, proplists:proplist()} | error.
dist_tls_opts(error, []) ->
    error;
dist_tls_opts(error, CmdLineArgs) ->
    {ok, merge_dist_tls_opts([], CmdLineArgs)};
dist_tls_opts({ok, TableOpts}, CmdLineArgs) ->
    {ok, merge_dist_tls_opts(TableOpts, CmdLineArgs)}.

%% The option file is consult'ed into this table by
%% `ssl_dist_sup:start_link/0', and the table only exists when OTP was given
%% `-ssl_dist_optfile'.
dist_tls_table_opts() ->
    try
        {ok, ets:lookup(ssl_dist_opts, server) ++ ets:lookup(ssl_dist_opts, client)}
    catch
        error:badarg -> error
    end.

dist_tls_cmd_line_args() ->
    case init:get_argument(ssl_dist_opt) of
        {ok, Args} -> lists:append(Args);
        _ -> []
    end.

%% @doc Merge the `-ssl_dist_opt' arguments into the option lists of the option
%% file the way `inet_tls_dist:get_ssl_options/1' does it: the command-line
%% options come first, the option file last, and a key that appears more than
%% once is resolved last-wins by `get_opt/3' - matching
%% `ssl_config:process_options/3', which reverses the list "so we get the last
%% set option if set twice". The option file therefore wins over the command
%% line, and a command-line option is only applied where the file omits the
%% key.
-spec merge_dist_tls_opts(proplists:proplist(), [string()]) -> proplists:proplist().
merge_dist_tls_opts(TableOpts, CmdLineArgs) ->
    [
        {Role, cmd_line_opts(Role, CmdLineArgs) ++ proplists:get_value(Role, TableOpts, [])}
     || Role <- [server, client]
    ].

%% Mirrors `inet_tls_dist:ssl_options/2': a command-line option is named
%% `<role>_<key>', and its value is atomized as `inet_tls_dist:atomize/1' does.
%% Options named for the other role are skipped, and only the keys this check
%% judges are looked up later, so unknown keys are carried along harmlessly.
cmd_line_opts(Role, [Opt, Value | Rest]) ->
    case role_opt(Role, Opt) of
        {ok, Key} -> [{Key, list_to_atom(Value)} | cmd_line_opts(Role, Rest)];
        skip -> cmd_line_opts(Role, Rest)
    end;
cmd_line_opts(_Role, _) ->
    [].

role_opt(Role, Opt) ->
    case string:prefix(Opt, atom_to_list(Role) ++ "_") of
        nomatch -> skip;
        Key -> {ok, list_to_atom(Key)}
    end.

dist_tls_unverified(Problems) ->
    ?SLOG(warning, #{
        msg => "erlang_distribution_tls_peer_verification_not_enforced",
        problems => Problems,
        node => node(),
        hint =>
            "Set {verify, verify_peer} in both the server and the client entries of the "
            "inet_tls optfile, with a cacertfile every node trusts, a certificate the "
            "client can present, and fail_if_no_peer_cert enabled on the server entry. "
            "See EMQX_SSL_DIST_OPTFILE."
    }).

is_dist_over_tls("inet_tls") ->
    true;
is_dist_over_tls("inet6_tls") ->
    true;
is_dist_over_tls(_) ->
    false.

check_server_opts(Opts) ->
    case check_verify(server, Opts) of
        [] -> check_client_cert_required(Opts);
        Problems -> Problems
    end.

check_client_opts(Opts) ->
    check_verify(client, Opts).

%% Only whether the peer is verified is judged. OTP defaults to verify_peer on
%% the client and verify_none on the server, so an entry that omits `verify' is
%% judged by the default of its role.
check_verify(Role, Opts) ->
    case get_opt(verify, Opts, default_verify(Role)) of
        verify_peer -> [];
        Verify -> [{Role, Verify}]
    end.

%% OTP defaults to verify_peer for the client and verify_none for the server.
default_verify(client) -> verify_peer;
default_verify(server) -> verify_none.

%% OTP implies fail_if_no_peer_cert=true on the server when verify=verify_peer.
check_client_cert_required(Opts) ->
    Default = get_opt(verify, Opts, default_verify(server)) =:= verify_peer,
    case get_opt(fail_if_no_peer_cert, Opts, Default) of
        true -> [];
        false -> [{server, fail_if_no_peer_cert_false}]
    end.

%% OTP folds a key that appears more than once in an optfile last-wins:
%% `ssl_dist_sup' stores the consulted list as it is, and
%% `ssl_config:process_options/3' reverses it before turning it into a map, "so
%% we get the last set option if set twice". `proplists:get_value/3' would take
%% the first one instead, and judge the entry by a value that OTP never uses.
get_opt(Key, Opts, Default) ->
    case [Value || {K, Value} <- Opts, K =:= Key] of
        [] -> Default;
        Values -> lists:last(Values)
    end.
