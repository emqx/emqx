%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_machine).

-export([
    start/0,
    graceful_shutdown/0,
    brutal_shutdown/0,
    is_ready/0,

    start_sys_apps/1,
    register_business_hooks/0,
    on_run_level/2,
    maybe_migrate_cluster/0,
    manage_business_apps/2,

    node_status/0,

    on_node_classify/1,

    is_cluster_ready/0,
    wait_cluster_ready/1
]).

-export([open_ports_check/0]).
-export([mria_lb_discover/0, mria_lb_custom_info/0, mria_lb_custom_info_check/1]).

-ifdef(TEST).
-export([create_plan/0]).
-endif.

-include_lib("kernel/include/inet.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("classy/include/classy.hrl").
-include_lib("emqx/include/emqx_machine_rl.hrl").

-define(ready_to_cluster, emqx_machine_ready_to_cluster).

%% @doc EMQX boot entrypoint.
start() ->
    ensure_valid_features(),
    emqx_mgmt_cli:load(),
    setup_vm(),
    ok = configure_otel_deps(),
    %% Setup EMQX hooks:
    _ = start_sys_apps(emqx:data_dir()),
    register_business_hooks(),
    %% Launch:
    ok = classy:start_system(),
    _ = wait_cluster_ready(infinity),
    ok.

-doc """
Return true if the machine has ran all necessary for the cluster run level.
""".
-spec is_cluster_ready() -> boolean().
is_cluster_ready() ->
    classy_boot:run_level(ready) >= ?emqx_rl_joinable.

-doc """
Don't use in the new code.

This function is specifically created for emqx_conf cluster RPC server,
which creates a strange circular dependency.
""".
-spec wait_cluster_ready(timeout()) -> ok | {error, stopping | timeout}.
wait_cluster_ready(Timeout) ->
    case optvar:read(?ready_to_cluster, Timeout) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, stopping};
        timeout ->
            {error, timeout}
    end.

setup_vm() ->
    os:set_signal(sighup, ignore),
    %% default is handle
    os:set_signal(sigterm, handle),
    ok = set_backtrace_depth().

%% Set up classy and mria. Return the closure of started apps.
-spec start_sys_apps(file:filename()) -> [atom()].
start_sys_apps(DataDir) ->
    %% Prepare classy directory:
    ClassyDir = filename:join(DataDir, "classy"),
    application:set_env(classy, table_dir, ClassyDir),
    ok = filelib:ensure_path(ClassyDir),
    %% Prepare mnesia directory:
    ok = application:set_env(mnesia, dir, filename:join([DataDir, "mnesia"])),
    %% Start classy app in dormant mode, which enables registration of hooks:
    {ok, Started1} = application:ensure_all_started(classy),
    %% Start mria in dormant mode:
    {ok, Started2} = application:ensure_all_started(mria),
    emqx_dist_tls_check:maybe_warn_dist_tls_verify(),
    %% Register classy hooks:
    %%   Cluster ID / site ID migration:
    _ = classy:on_node_init(fun ?MODULE:maybe_migrate_cluster/0, 100),
    %%   Cluster:
    _ = classy:pre_join(fun emqx_cluster:pre_join/4, 50),
    _ = classy:pre_kick(fun emqx_mgmt_api_ds:pre_kick/3, 50),
    _ = classy:on_node_classify(fun ?MODULE:on_node_classify/1, 50),
    _ = classy:on_kick_decided(fun emqx_cluster_rpc:on_kick_decided/3, 100),
    %%   Common actions
    _ = classy:on_run_level(
        fun ?MODULE:on_run_level/2,
        #{prio => 0, timeout => infinity}
    ),
    %%   Fallback, allows to connect to legacy nodes:
    _ = classy:fallback_get_cluster(fun mria_classy_migration:fallback_get_cluster/1),
    _ = classy:fallback_get_meta(fun mria_classy_migration:fallback_get_meta/2, 0),
    _ = classy:fallback_get_peer_nodes(fun mria_classy_migration:fallback_get_peer_nodes/1),
    %% Fallback, allows core nodes that restarted after upgrading to classy to reform the cluster:
    _ = classy:extra_sync_targets(fun mria_classy_migration:extra_sync_targets/1),
    %% Mria:
    %%
    %% Register mria callbacks that help to check compatibility of the
    %% replicant with the core node. Currently they rely on the exact
    %% match of the version of EMQX OTP application:
    mria_config:register_callback(lb_custom_info, fun ?MODULE:mria_lb_custom_info/0),
    mria_config:register_callback(lb_custom_info_check, fun ?MODULE:mria_lb_custom_info_check/1),
    configure_shard_transports(),
    set_mnesia_extra_diagnostic_checks(),
    mria_config:register_callback(heal_partition, fun emqx_broker_heal:on_autoheal/1),
    Started1 ++ Started2.

%% These hooks are registered separately to avoid starting unnecessary
%% applications in test:
register_business_hooks() ->
    _ = classy:on_run_level(
        fun ?MODULE:manage_business_apps/2,
        #{prio => 0, timeout => infinity}
    ),
    ok.

-doc false.
%% This function contains common actions that must always occur: in
%% live and test:
on_run_level(enter, ?classy_rl_single) ->
    %% Set site metadata:
    classy_site_metadata:c_set(
        emqx,
        #{vsn => get_emqx_vsn()}
    );
on_run_level(enter, ?emqx_rl_mria_declare) ->
    declare_mria_tables();
on_run_level(enter, ?emqx_rl_mria_ready) ->
    wait_mria_tables();
on_run_level(enter, ?emqx_rl_joinable) ->
    optvar:set(?ready_to_cluster, true);
on_run_level(leave, ?emqx_rl_joinable) ->
    optvar:set(?ready_to_cluster, false),
    optvar:unset(?ready_to_cluster);
on_run_level(_, _) ->
    ok.

manage_business_apps(enter, ?emqx_rl_boot) ->
    emqx_machine_boot:post_boot();
manage_business_apps(leave, ?emqx_rl_boot) ->
    emqx_machine_boot:stop_apps();
manage_business_apps(_, _) ->
    ok.

maybe_migrate_cluster() ->
    %% Use DS site ID:
    MaybeSite =
        case emqx_dsch_migrate:read_old() of
            {ok, #{schema := #{site := Site}}} ->
                Site;
            {ok, _} ->
                %% No old schema or no site in it: mint a fresh site ID
                undefined;
            {error, Reason} ->
                %% Minting a fresh site ID here would silently reset the
                %% durable-storage identity of the node (shard ownership,
                %% site metadata), so fail instead:
                ?SLOG(critical, #{msg => "failed_to_read_old_ds_schema", reason => Reason}),
                error({failed_to_read_old_ds_schema, Reason})
        end,
    %% Use hash of the mnesia schema cookie as the initial cluster ID:
    MaybeCluster = mria_classy_migration:maybe_cluster_id(),
    ?SLOG(notice, #{msg => "initialize_emqx_cluster", site => MaybeSite, cluster => MaybeCluster}),
    classy_node:maybe_init_the_site(MaybeSite, MaybeCluster).

graceful_shutdown() ->
    _ = classy:stop_system(),
    emqx_machine_terminator:graceful_wait().

%% only used when failed to boot
brutal_shutdown() ->
    init:stop().

set_backtrace_depth() ->
    {ok, Depth} = application:get_env(emqx_machine, backtrace_depth),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

%% @doc Return true if boot is complete.
-spec is_ready() -> boolean().
is_ready() ->
    classy_boot:run_level(ready) >= ?emqx_rl_ready.

node_status() ->
    emqx_utils_json:encode(#{
        backend => mria_rlog:backend(),
        role => mria_rlog:role()
    }).

%% TODO: deprecate custom_shard_transports parameter.
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

%% Forward classy autocluster results to mria
%% TODO: integrate mria directly with classy?
mria_lb_discover() ->
    classy:nodes(connected).

on_node_classify(#{emqx := #{vsn := Vsn}}) ->
    MyVsn = get_emqx_vsn(),
    [
        {emqx_vsn, Vsn}
        | case Vsn of
            MyVsn -> [emqx_same_vsn];
            _ -> []
        end
    ];
on_node_classify(#{}) ->
    [].

%% IMPR: avoid hardcoding the tables here. Find a way to let apps
%% manage their tables.

declare_mria_tables() ->
    emqx_conf_app:create_tables().

wait_mria_tables() ->
    emqx_conf_app:wait_for_tables().

%% TODO: Forward mria start/stop callbacks for autoheal

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
