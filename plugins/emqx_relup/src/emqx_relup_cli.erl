-module(emqx_relup_cli).

%% This is an example on how to extend `emqx ctl` with your own commands.

-export([cmd/1]).

cmd(["upgrade", TargetVsn]) ->
    case emqx_relup_main:upgrade(TargetVsn) of
        ok ->
            emqx_ctl:print("upgraded to ~s successfully", [TargetVsn]);
        {error, Reason} ->
            emqx_ctl:print("upgrade failed, reason: ~p", [Reason]);
        {error_vm_restarted, Reason} ->
            emqx_ctl:print("upgrade failed, emqx restarted, reason: ~p", [Reason])
    end;
cmd(_) ->
    emqx_ctl:usage([{"upgrade <TargetVsn>", "e.g. emqx ctl relup upgrade 5.7.1"}]).
