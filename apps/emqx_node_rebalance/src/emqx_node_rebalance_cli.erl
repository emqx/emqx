%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_cli).

%% APIs
-export([
    load/0,
    unload/0,
    cli/1
]).

load() ->
    emqx_ctl:register_command(rebalance, {?MODULE, cli}, []).

unload() ->
    emqx_ctl:unregister_command(rebalance).

cli(["start" | StartArgs]) ->
    case start_args(StartArgs) of
        {evacuation, Opts} ->
            case emqx_node_rebalance_evacuation:status() of
                disabled ->
                    ok = emqx_node_rebalance_evacuation:start(Opts),
                    emqx_ctl:print("Rebalance(evacuation) started~n"),
                    true;
                {enabled, _} ->
                    emqx_ctl:print("Rebalance is already enabled~n"),
                    false
            end;
        {purge, Opts} ->
            case emqx_node_rebalance_purge:start(Opts) of
                ok ->
                    emqx_ctl:print("Rebalance(purge) started~n"),
                    true;
                {error, Reason} ->
                    emqx_ctl:print("Rebalance(purge) start error: ~p~n", [Reason]),
                    false
            end;
        {rebalance, Opts} ->
            case emqx_node_rebalance:start(Opts) of
                ok ->
                    emqx_ctl:print("Rebalance started~n"),
                    true;
                {error, Reason} ->
                    emqx_ctl:print("Rebalance start error: ~p~n", [Reason]),
                    false
            end;
        {error, Error} ->
            emqx_ctl:print("Rebalance start error: ~s~n", [Error]),
            false
    end;
cli(["node-status", NodeStr]) ->
    case emqx_utils:safe_to_existing_atom(NodeStr, utf8) of
        {ok, Node} ->
            node_status(emqx_node_rebalance_status:local_status(Node));
        {error, _} ->
            emqx_ctl:print("Node status error: invalid node~n"),
            false
    end;
cli(["node-status"]) ->
    node_status(emqx_node_rebalance_status:local_status());
cli(["status"]) ->
    #{
        evacuations := Evacuations,
        purges := Purges,
        rebalances := Rebalances
    } = emqx_node_rebalance_status:global_status(),
    lists:foreach(
        fun({Node, Status}) ->
            emqx_ctl:print(
                "--------------------------------------------------------------------~n"
            ),
            emqx_ctl:print(
                "Node ~p: evacuation~n~s",
                [Node, emqx_node_rebalance_status:format_local_status(Status)]
            )
        end,
        Evacuations
    ),
    lists:foreach(
        fun({Node, Status}) ->
            emqx_ctl:print(
                "--------------------------------------------------------------------~n"
            ),
            emqx_ctl:print(
                "Node ~p: purge~n~s",
                [Node, emqx_node_rebalance_status:format_local_status(Status)]
            )
        end,
        Purges
    ),
    lists:foreach(
        fun({Node, Status}) ->
            emqx_ctl:print(
                "--------------------------------------------------------------------~n"
            ),
            emqx_ctl:print(
                "Node ~p: rebalance coordinator~n~s",
                [Node, emqx_node_rebalance_status:format_coordinator_status(Status)]
            )
        end,
        Rebalances
    );
cli(["stop"]) ->
    Checks =
        [
            {evacuation, fun emqx_node_rebalance_evacuation:status/0,
                fun emqx_node_rebalance_evacuation:stop/0},
            {purge, fun emqx_node_rebalance_purge:status/0, fun emqx_node_rebalance_purge:stop/0}
        ],
    case do_stop(Checks) of
        ok ->
            true;
        disabled ->
            case emqx_node_rebalance:status() of
                {enabled, _} ->
                    ok = emqx_node_rebalance:stop(),
                    emqx_ctl:print("Rebalance stopped~n"),
                    true;
                disabled ->
                    emqx_ctl:print("Rebalance is already disabled~n"),
                    false
            end
    end;
cli(_) ->
    emqx_ctl:usage(
        [
            {
                "rebalance start --evacuation \\\n"
                "    [--wait-health-check Secs] \\\n"
                "    [--redirect-to \"Host1:Port1 Host2:Port2 ...\"] \\\n"
                "    [--conn-evict-rate CountPerSec] \\\n"
                "    [--migrate-to \"node1@host1 node2@host2 ...\"] \\\n"
                "    [--wait-takeover Secs] \\\n"
                "    [--sess-evict-rate CountPerSec]",
                "Start current node evacuation with optional server redirect to the specified servers"
            },

            %% TODO: uncomment after we officially release the feature.
            %% {
            %%     "rebalance start --purge \\\n"
            %%     "    [--purge-rate CountPerSec]",
            %%     "Start purge on all running nodes in the cluster"
            %% },

            {
                "rebalance start \\\n"
                "    [--nodes \"node1@host1 node2@host2\"] \\\n"
                "    [--wait-health-check Secs] \\\n"
                "    [--conn-evict-rate ConnPerSec] \\\n"
                "    [--conn-evict-rpc-timeout Secs] \\\n"
                "    [--abs-conn-threshold Count] \\\n"
                "    [--rel-conn-threshold Fraction] \\\n"
                "    [--wait-takeover Secs] \\\n"
                "    [--sess-evict-rate CountPerSec] \\\n"
                "    [--sess-evict-rpc-timeout Secs] \\\n"
                "    [--abs-sess-threshold Count] \\\n"
                "    [--rel-sess-threshold Fraction]",
                "Start rebalance on the specified nodes using the current node as the coordinator"
            },

            {"rebalance node-status", "Get current node rebalance status"},

            {"rebalance node-status \"node1@host1\"", "Get remote node rebalance status"},

            {"rebalance status",
                "Get statuses of all current rebalance/evacuation processes across the cluster"},

            {"rebalance stop", "Stop node rebalance"}
        ]
    ).

node_status(NodeStatus) ->
    case NodeStatus of
        {Process, Status} when
            Process =:= evacuation;
            Process =:= purge;
            Process =:= rebalance
        ->
            emqx_ctl:print(
                "Rebalance type: ~p~n~s~n",
                [Process, emqx_node_rebalance_status:format_local_status(Status)]
            );
        disabled ->
            emqx_ctl:print("Rebalance disabled~n");
        Other ->
            emqx_ctl:print("Error detecting rebalance status: ~p~n", [Other])
    end.

start_args(Args) ->
    case collect_args(Args, #{}) of
        {ok, #{"--evacuation" := true} = Collected} ->
            case validate_evacuation(maps:to_list(Collected), #{}) of
                {ok, Validated} ->
                    {evacuation, Validated};
                {error, _} = Error ->
                    Error
            end;
        {ok, #{"--purge" := true} = Collected} ->
            case validate_purge(maps:to_list(Collected), #{}) of
                {ok, Validated} ->
                    {purge, Validated};
                {error, _} = Error ->
                    Error
            end;
        {ok, #{} = Collected} ->
            case validate_rebalance(maps:to_list(Collected), #{}) of
                {ok, Validated} ->
                    {rebalance, Validated};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

collect_args([], Map) ->
    {ok, Map};
%% evacuation
collect_args(["--evacuation" | Args], Map) ->
    collect_args(Args, Map#{"--evacuation" => true});
collect_args(["--redirect-to", ServerReference | Args], Map) ->
    collect_args(Args, Map#{"--redirect-to" => ServerReference});
collect_args(["--migrate-to", MigrateTo | Args], Map) ->
    collect_args(Args, Map#{"--migrate-to" => MigrateTo});
%% purge
collect_args(["--purge" | Args], Map) ->
    collect_args(Args, Map#{"--purge" => true});
collect_args(["--purge-rate", PurgeRate | Args], Map) ->
    collect_args(Args, Map#{"--purge-rate" => PurgeRate});
%% rebalance
collect_args(["--nodes", Nodes | Args], Map) ->
    collect_args(Args, Map#{"--nodes" => Nodes});
collect_args(["--abs-conn-threshold", AbsConnThres | Args], Map) ->
    collect_args(Args, Map#{"--abs-conn-threshold" => AbsConnThres});
collect_args(["--rel-conn-threshold", RelConnThres | Args], Map) ->
    collect_args(Args, Map#{"--rel-conn-threshold" => RelConnThres});
collect_args(["--abs-sess-threshold", AbsSessThres | Args], Map) ->
    collect_args(Args, Map#{"--abs-sess-threshold" => AbsSessThres});
collect_args(["--rel-sess-threshold", RelSessThres | Args], Map) ->
    collect_args(Args, Map#{"--rel-sess-threshold" => RelSessThres});
collect_args(["--conn-evict-rpc-timeout", ConnEvictRpcTimeout | Args], Map) ->
    collect_args(Args, Map#{"--conn-evict-rpc-timeout" => ConnEvictRpcTimeout});
collect_args(["--sess-evict-rpc-timeout", SessEvictRpcTimeout | Args], Map) ->
    collect_args(Args, Map#{"--sess-evict-rpc-timeout" => SessEvictRpcTimeout});
%% common
collect_args(["--wait-health-check", WaitHealthCheck | Args], Map) ->
    collect_args(Args, Map#{"--wait-health-check" => WaitHealthCheck});
collect_args(["--conn-evict-rate", ConnEvictRate | Args], Map) ->
    collect_args(Args, Map#{"--conn-evict-rate" => ConnEvictRate});
collect_args(["--wait-takeover", WaitTakeover | Args], Map) ->
    collect_args(Args, Map#{"--wait-takeover" => WaitTakeover});
collect_args(["--sess-evict-rate", SessEvictRate | Args], Map) ->
    collect_args(Args, Map#{"--sess-evict-rate" => SessEvictRate});
%% fallback
collect_args(Args, _Map) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

validate_evacuation([], Map) ->
    {ok, Map};
validate_evacuation([{"--evacuation", _} | Rest], Map) ->
    validate_evacuation(Rest, Map);
validate_evacuation([{"--wait-health-check", _} | _] = Opts, Map) ->
    validate_pos_int(wait_health_check, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--redirect-to", ServerReference} | Rest], Map) ->
    validate_evacuation(Rest, Map#{server_reference => list_to_binary(ServerReference)});
validate_evacuation([{"--conn-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(conn_evict_rate, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--sess-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(sess_evict_rate, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--wait-takeover", _} | _] = Opts, Map) ->
    validate_pos_int(wait_takeover, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--migrate-to", MigrateTo} | Rest], Map) ->
    case strings_to_atoms(string:tokens(MigrateTo, ", ")) of
        {_, Invalid} when Invalid =/= [] ->
            {error, io_lib:format("invalid --migrate-to, invalid nodes: ~p", [Invalid])};
        {Nodes, []} ->
            case emqx_node_rebalance_evacuation:available_nodes(Nodes) of
                [] ->
                    {error, "invalid --migrate-to, no nodes"};
                Nodes ->
                    validate_evacuation(Rest, Map#{migrate_to => Nodes});
                OtherNodes ->
                    {error,
                        io_lib:format(
                            "invalid --migrate-to, unavailable nodes: ~p",
                            [Nodes -- OtherNodes]
                        )}
            end
    end;
validate_evacuation(Rest, _Map) ->
    {error, io_lib:format("unknown evacuation arguments: ~p", [Rest])}.

validate_purge([], Map) ->
    {ok, Map};
validate_purge([{"--purge", _} | Rest], Map) ->
    validate_purge(Rest, Map);
validate_purge([{"--purge-rate", _} | _] = Opts, Map) ->
    validate_pos_int(purge_rate, Opts, Map, fun validate_purge/2);
validate_purge(Rest, _Map) ->
    {error, io_lib:format("unknown purge arguments: ~p", [Rest])}.

validate_rebalance([], Map) ->
    {ok, Map};
validate_rebalance([{"--wait-health-check", _} | _] = Opts, Map) ->
    validate_pos_int(wait_health_check, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--conn-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(conn_evict_rate, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--conn-evict-rpc-timeout", _} | _] = Opts, Map) ->
    validate_sec_timeout(conn_evict_rpc_timeout, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--sess-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(sess_evict_rate, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--sess-evict-rpc-timeout", _} | _] = Opts, Map) ->
    validate_sec_timeout(sess_evict_rpc_timeout, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--abs-conn-threshold", _} | _] = Opts, Map) ->
    validate_pos_int(abs_conn_threshold, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--rel-conn-threshold", _} | _] = Opts, Map) ->
    validate_fraction(rel_conn_threshold, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--abs-sess-threshold", _} | _] = Opts, Map) ->
    validate_pos_int(abs_sess_threshold, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--rel-sess-threshold", _} | _] = Opts, Map) ->
    validate_fraction(rel_sess_threshold, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--wait-takeover", _} | _] = Opts, Map) ->
    validate_pos_int(wait_takeover, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--nodes", NodeStr} | Rest], Map) ->
    case strings_to_atoms(string:tokens(NodeStr, ", ")) of
        {_, Invalid} when Invalid =/= [] ->
            {error, io_lib:format("invalid --nodes, invalid nodes: ~p", [Invalid])};
        {Nodes, []} ->
            case emqx_node_rebalance:available_nodes(Nodes) of
                [] ->
                    {error, "invalid --nodes, no nodes"};
                Nodes ->
                    validate_rebalance(Rest, Map#{nodes => Nodes});
                OtherNodes ->
                    {error,
                        io_lib:format(
                            "invalid --nodes, unavailable nodes: ~p",
                            [Nodes -- OtherNodes]
                        )}
            end
    end;
validate_rebalance(Rest, _Map) ->
    {error, io_lib:format("unknown rebalance arguments: ~p", [Rest])}.

validate_fraction(Name, [{OptionName, Value} | Rest], Map, Next) ->
    case string:to_float(Value) of
        {Num, ""} when Num > 1.0 ->
            Next(Rest, Map#{Name => Num});
        _ ->
            {error, "invalid " ++ OptionName ++ " value"}
    end.

validate_pos_int(Name, [{OptionName, Value} | Rest], Map, Next) ->
    case string:to_integer(Value) of
        {Int, ""} when Int > 0 ->
            Next(Rest, Map#{Name => Int});
        _ ->
            {error, "invalid " ++ OptionName ++ " value"}
    end.

validate_sec_timeout(Name, [{OptionName, Value} | Rest], Map, Next) ->
    case string:to_integer(Value) of
        {Int, ""} when Int > 0 ->
            Next(Rest, Map#{Name => Int * 1000});
        _ ->
            {error, "invalid " ++ OptionName ++ " value"}
    end.

strings_to_atoms(Strings) ->
    strings_to_atoms(Strings, [], []).

strings_to_atoms([], Atoms, Invalid) ->
    {lists:reverse(Atoms), lists:reverse(Invalid)};
strings_to_atoms([Str | Rest], Atoms, Invalid) ->
    case emqx_utils:safe_to_existing_atom(Str, utf8) of
        {ok, Atom} ->
            strings_to_atoms(Rest, [Atom | Atoms], Invalid);
        {error, _} ->
            strings_to_atoms(Rest, Atoms, [Str | Invalid])
    end.

do_stop([{Type, Check, Stop} | Rest]) ->
    case Check() of
        {enabled, _} ->
            ok = Stop(),
            emqx_ctl:print("Rebalance(~s) stopped~n", [Type]),
            ok;
        disabled ->
            do_stop(Rest)
    end;
do_stop([]) ->
    disabled.
