-module(emqx_relup_cli).

-export([cmd/1]).

cmd(["upgrade", TarballPath]) ->
    case emqx_relup_main:upgrade(TarballPath) of
        ok ->
            emqx_ctl:print("upgrade complete~n");
        {error, Reason} ->
            emqx_ctl:print("upgrade failed, reason: ~p~n", [Reason]);
        {error_vm_restarted, Reason} ->
            emqx_ctl:print("upgrade failed, emqx restarted, reason: ~p~n", [Reason])
    end;
cmd(["list-supported-paths"]) ->
    case emqx_relup_handler:list_supported_paths() of
        [] ->
            emqx_ctl:print("no supported upgrade paths in priv catalog~n");
        Paths ->
            [
                emqx_ctl:print("~s -> ~s~n", [F, T])
             || #{from_version := F, target_version := T} <- Paths
            ]
    end;
cmd(["status"]) ->
    emqx_ctl:print("~ts~n", [emqx_relup_main:get_latest_upgrade_status()]);
cmd(["logs"]) ->
    case emqx_relup_main:get_all_upgrade_logs() of
        [] ->
            emqx_ctl:print("no upgrade history on this node~n");
        Logs ->
            lists:foreach(fun print_log/1, Logs)
    end;
cmd(["logs-clear"]) ->
    ok = emqx_relup_main:delete_all_upgrade_logs(),
    emqx_ctl:print("cleared all upgrade logs~n");
cmd(_) ->
    emqx_ctl:usage([
        {"relup upgrade <TarballPath>",
            "Upgrade using the EMQX target tarball at <TarballPath>. "
            "A `<TarballPath>.sha256` sidecar must sit next to it. "
            "Target version is read from the tarball's "
            "`releases/emqx_vars` (`REL_VSN`)."},
        {"relup list-supported-paths", "List the {From, Target} hops the priv catalog supports"},
        {"relup status",
            "Print 'in-progress' if an upgrade is currently running on this "
            "node, otherwise 'idle'."},
        {"relup logs", "Print this node's upgrade history (one row per attempt)."},
        {"relup logs-clear", "Wipe this node's upgrade log table."}
    ]).

print_log(#{
    started_at := Started,
    finished_at := Finished,
    from_vsn := From,
    target_vsn := Target,
    status := Status,
    result := Result
}) ->
    emqx_ctl:print(
        "started=~ts finished=~ts ~ts -> ~ts status=~p result=~0p~n",
        [Started, Finished, From, Target, Status, Result]
    ).
