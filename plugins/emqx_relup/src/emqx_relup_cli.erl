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
cmd(_) ->
    emqx_ctl:usage([
        {"relup upgrade <TarballPath>",
            "Upgrade using the EMQX target tarball at <TarballPath>. "
            "A `<TarballPath>.sha256` sidecar must sit next to it. "
            "Target version is read from the tarball's "
            "`releases/emqx_vars` (`REL_VSN`)."},
        {"relup list-supported-paths", "List the {From, Target} hops the priv catalog supports"}
    ]).
