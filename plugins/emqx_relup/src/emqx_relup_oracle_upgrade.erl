%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_relup_oracle_upgrade).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-export([
    restart_oracle_connectors/0
]).

%% One-off helper for the 5.10.4 -> 5.10.5 relup hop.
restart_oracle_connectors() ->
    Mod = emqx_resource,
    ConnResIds =
        lists:filter(
            fun(ConnResId) -> is_running_resource(Mod, ConnResId) end,
            Mod:list_instances_by_type(emqx_oracle)
        ),
    lists:foreach(fun Mod:restart/1, ConnResIds),
    ok.

is_running_resource(Mod, ResId) ->
    case Mod:get_instance(ResId) of
        {ok, _, #{status := stopped}} ->
            false;
        {ok, _, _} ->
            true;
        _ ->
            false
    end.
