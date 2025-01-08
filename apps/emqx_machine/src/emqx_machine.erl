%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine).

-export([
    start/0,
    graceful_shutdown/0,
    brutal_shutdown/0,
    is_ready/0,

    node_status/0,
    update_vips/0
]).

-export([open_ports_check/0]).
-export([mria_lb_custom_info/0, mria_lb_custom_info_check/1]).

-ifdef(TEST).
-export([create_plan/0]).
-endif.

-include_lib("kernel/include/inet.hrl").
-include_lib("emqx/include/logger.hrl").

%% @doc EMQX boot entrypoint.
start() ->
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
    ok = start_sysmon(),
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

start_sysmon() ->
    _ = application:load(system_monitor),
    application:set_env(system_monitor, node_status_fun, {?MODULE, node_status}),
    application:set_env(system_monitor, status_checks, [{?MODULE, update_vips, false, 10}]),
    case application:get_env(system_monitor, db_hostname) of
        {ok, [_ | _]} ->
            application:set_env(system_monitor, callback_mod, system_monitor_pg),
            _ = application:ensure_all_started(system_monitor, temporary),
            ok;
        _ ->
            %% If there is no sink for the events, there is no reason
            %% to run system_monitor_top, ignore start
            ok
    end.

node_status() ->
    emqx_utils_json:encode(#{
        backend => mria_rlog:backend(),
        role => mria_rlog:role()
    }).

update_vips() ->
    system_monitor:add_vip(mria_status:shards_up()).

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

-if(?EMQX_RELEASE_EDITION == ee).
configure_otel_deps() ->
    emqx_otel_app:configure_otel_deps().
-else.
configure_otel_deps() ->
    ok.
-endif.

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

get_emqx_vsn() ->
    case application:get_key(emqx, vsn) of
        {ok, Vsn} ->
            Vsn;
        undefined ->
            undefined
    end.
