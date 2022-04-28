%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_node_rebalance_cli).

%% APIs
-export([ load/0
        , unload/0
        , cli/1
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
    Node = list_to_atom(NodeStr),
    node_status(emqx_node_rebalance_status:local_status(Node));
cli(["node-status"]) ->
    node_status(emqx_node_rebalance_status:local_status());
cli(["status"]) ->
    #{evacuations := Evacuations,
      rebalances := Rebalances} = emqx_node_rebalance_status:global_status(),
    lists:foreach(
      fun({Node, Status}) ->
        emqx_ctl:print("--------------------------------------------------------------------~n"),
        emqx_ctl:print("Node ~p: evacuation~n~s",
                       [Node, emqx_node_rebalance_status:format_local_status(Status)])
      end,
      Evacuations),
    lists:foreach(
      fun({Node, Status}) ->
        emqx_ctl:print("--------------------------------------------------------------------~n"),
        emqx_ctl:print("Node ~p: rebalance coordinator~n~s",
                       [Node, emqx_node_rebalance_status:format_coordinator_status(Status)])
      end,
      Rebalances);
cli(["stop"]) ->
    case emqx_node_rebalance_evacuation:status() of
        {enabled, _} ->
            ok = emqx_node_rebalance_evacuation:stop(),
            emqx_ctl:print("Rebalance(evacuation) stopped~n"),
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
      [{"rebalance start --evacuation \\\n"
        "    [--redirect-to \"Host1:Port1 Host2:Port2 ...\"] \\\n"
        "    [--conn-evict-rate CountPerSec] \\\n"
        "    [--migrate-to \"node1@host1 node2@host2 ...\"] \\\n"
        "    [--wait-takeover Secs] \\\n"
        "    [--sess-evict-rate CountPerSec]",
        "Start current node evacuation with optional server redirect to the specified servers"},

       {"rebalance start \\\n"
        "    [--nodes \"node1@host1 node2@host2\"] \\\n"
        "    [--wait-health-check Secs] \\\n"
        "    [--conn-evict-rate ConnPerSec] \\\n"
        "    [--abs-conn-threshold Count] \\\n"
        "    [--rel-conn-threshold Fraction] \\\n"
        "    [--conn-evict-rate ConnPerSec] \\\n"
        "    [--wait-takeover Secs] \\\n"
        "    [--sess-evict-rate CountPerSec] \\\n"
        "    [--abs-sess-threshold Count] \\\n"
        "    [--rel-sess-threshold Fraction]",
        "Start current node evacuation with optional server redirect to the specified servers"},

       {"rebalance node-status",
        "Get current node rebalance status"},

       {"rebalance node-status \"node1@host1\"",
        "Get remote node rebalance status"},

       {"rebalance status",
        "Get statuses of all current rebalance/evacuation processes across the cluster"},

       {"rebalance stop",
        "Stop node rebalance"}]).

node_status(NodeStatus) ->
    case NodeStatus of
        {Process, Status} when Process =:= evacuation orelse Process =:= rebalance ->
            emqx_ctl:print("Rebalance type: ~p~n~s~n",
                           [Process, emqx_node_rebalance_status:format_local_status(Status)]);
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
                {error, _} = Error -> Error
            end;
        {ok, #{} = Collected} ->
            case validate_rebalance(maps:to_list(Collected), #{}) of
                {ok, Validated} ->
                    {rebalance, Validated};
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error
    end.

collect_args([], Map) -> {ok, Map};

%% evacuation
collect_args(["--evacuation" | Args], Map) ->
    collect_args(Args, Map#{"--evacuation" => true});
collect_args(["--redirect-to", ServerReference | Args], Map) ->
    collect_args(Args, Map#{"--redirect-to" => ServerReference});
collect_args(["--migrate-to", MigrateTo | Args], Map) ->
    collect_args(Args, Map#{"--migrate-to" => MigrateTo});
%% rebalance
collect_args(["--nodes", Nodes | Args], Map) ->
    collect_args(Args, Map#{"--nodes" => Nodes});
collect_args(["--wait-health-check", WaitHealthCheck | Args], Map) ->
    collect_args(Args, Map#{"--wait-health-check" => WaitHealthCheck});
collect_args(["--abs-conn-threshold", AbsConnThres | Args], Map) ->
    collect_args(Args, Map#{"--abs-conn-threshold" => AbsConnThres});
collect_args(["--rel-conn-threshold", RelConnThres | Args], Map) ->
    collect_args(Args, Map#{"--rel-conn-threshold" => RelConnThres});
collect_args(["--abs-sess-threshold", AbsSessThres | Args], Map) ->
    collect_args(Args, Map#{"--abs-sess-threshold" => AbsSessThres});
collect_args(["--rel-sess-threshold", RelSessThres | Args], Map) ->
    collect_args(Args, Map#{"--rel-sess-threshold" => RelSessThres});
%% common
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
validate_evacuation([{"--redirect-to", ServerReference} | Rest], Map) ->
    validate_evacuation(Rest, Map#{server_reference => list_to_binary(ServerReference)});
validate_evacuation([{"--conn-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(conn_evict_rate, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--sess-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(sess_evict_rate, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--wait-takeover", _} | _] = Opts, Map) ->
    validate_pos_int(wait_takeover, Opts, Map, fun validate_evacuation/2);
validate_evacuation([{"--migrate-to", MigrateTo} | Rest], Map) ->
    Nodes = lists:map(fun list_to_atom/1, string:tokens(MigrateTo, ", ")),
    case emqx_node_rebalance_evacuation:available_nodes(Nodes) of
        [] ->
            {error, "invalid --migrate-to, no nodes"};
        Nodes ->
            validate_evacuation(Rest, Map#{migrate_to => Nodes});
        OtherNodes ->
            {error,
             io_lib:format("invalid --migrate-to, unavailable nodes: ~p",
                           [Nodes -- OtherNodes])}
    end;
validate_evacuation(Rest, _Map) ->
    {error, io_lib:format("unknown evacuation arguments: ~p", [Rest])}.

validate_rebalance([], Map) ->
    {ok, Map};
validate_rebalance([{"--wait-health-check", _} | _] = Opts, Map) ->
    validate_pos_int(wait_health_check, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--conn-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(conn_evict_rate, Opts, Map, fun validate_rebalance/2);
validate_rebalance([{"--sess-evict-rate", _} | _] = Opts, Map) ->
    validate_pos_int(sess_evict_rate, Opts, Map, fun validate_rebalance/2);
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
    Nodes = lists:map(fun list_to_atom/1, string:tokens(NodeStr, ", ")),
    case emqx_node_rebalance:available_nodes(Nodes) of
        [] ->
            {error, "invalid --nodes, no nodes"};
        Nodes ->
            validate_rebalance(Rest, Map#{nodes => Nodes});
        OtherNodes ->
            {error,
             io_lib:format("invalid --nodes, unavailable nodes: ~p",
                           [Nodes -- OtherNodes])}
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
