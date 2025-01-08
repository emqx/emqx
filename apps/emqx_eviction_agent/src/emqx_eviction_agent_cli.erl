%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_cli).

%% APIs
-export([
    load/0,
    unload/0,
    cli/1
]).

load() ->
    emqx_ctl:register_command(eviction, {?MODULE, cli}, []).

unload() ->
    emqx_ctl:unregister_command(eviction).

cli(["status"]) ->
    case emqx_eviction_agent:status() of
        disabled ->
            emqx_ctl:print("Eviction status: disabled~n");
        {enabled, _Stats} ->
            emqx_ctl:print("Eviction status: enabled~n")
    end;
cli(_) ->
    emqx_ctl:usage(
        [{"eviction status", "Get current node eviction status"}]
    ).
