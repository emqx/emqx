%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_machine).

-export([
    start/0,
    graceful_shutdown/0,
    brutal_shutdown/0,
    is_ready/0,

    node_status/0
]).

-export([open_ports_check/0]).
-export([mria_lb_custom_info/0, mria_lb_custom_info_check/1]).
-export([check_dist_tls_verify/2, merge_dist_tls_opts/2, dist_tls_opts/2]).

-ifdef(TEST).
-export([create_plan/0]).
-endif.

-include_lib("kernel/include/inet.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% @doc EMQX boot entrypoint.
start() ->
    %% Refuse cluster joins until emqx_machine_boot:post_boot/0 declares
    %% boot complete; a join restarts mria, which is fatal to apps that
    %% are still starting.
    ok = emqx_cluster:set_booting(true),
    ensure_valid_features(),
    emqx_mgmt_cli:load(),
    case os:type() of
        {win32, nt} ->
            ok;
        _Nix ->
            os:set_signal(sighup, ignore),
            %% default is handle
            os:set_signal(sigterm, handle)
    end,
    ok = set_backtrace_depth(),
    configure_shard_transports(),
    set_mnesia_extra_diagnostic_checks(),
    ok = configure_otel_deps(),
    %% Register mria callbacks that help to check compatibility of the
    %% replicant with the core node. Currently they rely on the exact
    %% match of the version of EMQX OTP application:
    _ = application:load(mria),
    _ = application:load(emqx),
    mria_config:register_callback(lb_custom_info, fun ?MODULE:mria_lb_custom_info/0),
    mria_config:register_callback(lb_custom_info_check, fun ?MODULE:mria_lb_custom_info_check/1),
    mria_config:register_callback(heal_partition, fun emqx_broker_heal:on_autoheal/1),
    maybe_warn_dist_tls_verify(),
    ekka:start(),
    ok.

graceful_shutdown() ->
    emqx_machine_terminator:graceful_wait().

%% only used when failed to boot
brutal_shutdown() ->
    init:stop().

set_backtrace_depth() ->
    {ok, Depth} = application:get_env(emqx_machine, backtrace_depth),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

%% @doc Return true if boot is complete.
is_ready() ->
    emqx_machine_terminator:is_running().

node_status() ->
    emqx_utils_json:encode(#{
        backend => mria_rlog:backend(),
        role => mria_rlog:role()
    }).

configure_shard_transports() ->
    ShardTransports = application:get_env(emqx_machine, custom_shard_transports, #{}),
    lists:foreach(
        fun({ShardBin, Transport}) ->
            ShardName = binary_to_existing_atom(ShardBin),
            mria_config:set_shard_transport(ShardName, Transport)
        end,
        maps:to_list(ShardTransports)
    ).

set_mnesia_extra_diagnostic_checks() ->
    Checks = [{check_open_ports, ok, fun ?MODULE:open_ports_check/0}],
    mria_config:set_extra_mnesia_diagnostic_checks(Checks),
    ok.

configure_otel_deps() ->
    emqx_otel_app:configure_otel_deps().

-define(PORT_PROBE_TIMEOUT, 10_000).
open_ports_check() ->
    Plan = create_plan(),
    %% 2 ports to check: ekka/epmd and gen_rpc
    Timeout = 2 * ?PORT_PROBE_TIMEOUT + 5_000,
    try emqx_utils:pmap(fun do_check/1, Plan, Timeout) of
        Results ->
            verify_results(Results)
    catch
        Kind:Reason:Stacktrace ->
            #{
                msg => "error probing ports",
                exception => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }
    end.

verify_results(Results0) ->
    Errors = [
        R
     || R = {_Node, #{status := Status}} <- Results0,
        Status =/= ok
    ],
    case Errors of
        [] ->
            %% all ok
            ok;
        _ ->
            Results1 = maps:from_list(Results0),
            #{results => Results1, msg => "some ports are unreachable"}
    end.

create_plan() ->
    %% expected core nodes according to mnesia schema
    OtherNodes = mnesia:system_info(db_nodes) -- [node()],
    lists:map(
        fun(N) ->
            IPs = node_to_ips(N),
            {_GenRPCMod, GenRPCPort} = gen_rpc_helper:get_client_config_per_node(N),
            %% 0 or 1 result
            EkkaEPMDPort = get_ekka_epmd_port(IPs),
            {N, #{
                resolved_ips => IPs,
                ports_to_check => [GenRPCPort | EkkaEPMDPort]
            }}
        end,
        OtherNodes
    ).

get_ekka_epmd_port([IP | _]) ->
    %% we're currently only checking the first IP, if there are many
    case erl_epmd:names(IP) of
        {ok, NamePorts} ->
            choose_emqx_epmd_port(NamePorts);
        _ ->
            []
    end;
get_ekka_epmd_port([]) ->
    %% failed to get?
    [].

%% filter out remsh and take the first emqx port as epmd/ekka port
choose_emqx_epmd_port([{"emqx" ++ _, Port} | _]) ->
    [Port];
choose_emqx_epmd_port([{_Name, _Port} | Rest]) ->
    choose_emqx_epmd_port(Rest);
choose_emqx_epmd_port([]) ->
    [].

do_check({Node, #{resolved_ips := []} = Plan}) ->
    {Node, Plan#{status => failed_to_resolve_ip}};
do_check({Node, #{resolved_ips := [IP | _]} = Plan}) ->
    %% check other IPs too?
    PortsToCheck = maps:get(ports_to_check, Plan),
    PortStatus0 = lists:map(fun(P) -> is_tcp_port_open(IP, P) end, PortsToCheck),
    case lists:all(fun(IsOpen) -> IsOpen end, PortStatus0) of
        true ->
            {Node, Plan#{status => ok}};
        false ->
            PortStatus1 = maps:from_list(lists:zip(PortsToCheck, PortStatus0)),
            {Node, Plan#{status => bad_ports, open_ports => PortStatus1}}
    end.

node_to_ips(Node) ->
    NodeBin0 = atom_to_binary(Node),
    HostOrIP = re:replace(NodeBin0, <<"^.+@">>, <<"">>, [{return, list}]),
    AddressType = resolve_dist_address_type(),
    case inet:gethostbyname(HostOrIP, AddressType) of
        {ok, #hostent{h_addr_list = AddrList}} ->
            AddrList;
        _ ->
            []
    end.

is_tcp_port_open(IP, Port) ->
    case gen_tcp:connect(IP, Port, [], ?PORT_PROBE_TIMEOUT) of
        {ok, P} ->
            gen_tcp:close(P),
            true;
        _ ->
            false
    end.

resolve_dist_address_type() ->
    ProtoDistStr = os:getenv("EKKA_PROTO_DIST_MOD", "inet_tcp"),
    case ProtoDistStr of
        "inet_tcp" ->
            inet;
        "inet6_tcp" ->
            inet6;
        "inet_tls" ->
            inet;
        "inet6_tls" ->
            inet6;
        _ ->
            inet
    end.

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
%% application is up, so this runs after the distribution listener exists: it
%% cannot prevent the listener from being created, but under a non-legacy
%% security profile it stops the node before it can run or join as a booted
%% member. It judges the options the VM actually applies, so unlike the text
%% match in bin/emqx it also covers a 'verify' that was computed at read time,
%% an entry that omits 'verify' and falls back to an OTP default, and a
%% 'verify' that only a '-ssl_dist_opt' argument sets.
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
    Data = #{
        msg => "erlang_distribution_tls_peer_verification_not_enforced",
        problems => Problems,
        node => node(),
        hint =>
            "Set {verify, verify_peer} in both the server and the client entries of the "
            "inet_tls optfile, with a cacertfile every node trusts, a certificate the "
            "client can present, and fail_if_no_peer_cert enabled on the server entry. "
            "See EMQX_SSL_DIST_OPTFILE."
    },
    case emqx_security_profile:policy(dist_tls_unverified) of
        deny ->
            ?SLOG(error, Data),
            %% Same pattern as ensure_valid_features/0.
            exit_loop(1);
        warn ->
            ?SLOG(warning, Data)
    end.

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

%% Note: this function is stored in the Mria's application environment
mria_lb_custom_info() ->
    get_emqx_vsn().

%% Note: this function is stored in the Mria's application environment
%% This function is only evaluated by replicant nodes.
%% Should return `true' if the input node version may be connected to by the current node.
mria_lb_custom_info_check(undefined) ->
    false;
mria_lb_custom_info_check(OtherVsn) ->
    get_emqx_vsn() =:= OtherVsn.

get_emqx_vsn() ->
    case application:get_key(emqx, vsn) of
        {ok, Vsn} ->
            Vsn;
        undefined ->
            undefined
    end.

ensure_valid_features() ->
    try
        Info = emqx_machine_features:info(),
        ?SLOG(notice, Info#{msg => "feature_gates_resolved"}),
        ok
    catch
        exit:#{} = Context ->
            ?tp(critical, "invalid_feature_specification", Context),
            exit_loop(1)
    end.

exit_loop(ExitCode) ->
    timer:sleep(100),
    init:stop(ExitCode),
    exit_loop(ExitCode).
