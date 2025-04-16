-module(emqx_mcp_gateway_cli).

%% This is an example on how to extend `emqx ctl` with your own commands.

-export([cmd/1]).

cmd(["arg1", "arg2"]) ->
    emqx_ctl:print("ok");
cmd(_) ->
    emqx_ctl:usage([{"cmd arg1 arg2", "cmd demo"}]).
