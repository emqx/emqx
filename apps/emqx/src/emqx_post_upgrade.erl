%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_post_upgrade).

%% Example of a hot upgrade callback function.
%% PR#12765
% -export([
%     pr12765_update_stats_timer/1,
%     pr20000_ensure_sup_started/3
% ]).

%% Please ensure that every callback function is reentrant.
%% This way, users can attempt upgrade multiple times if an issue arises.
%%
% pr12765_update_stats_timer(_FromVsn) ->
%     emqx_stats:update_interval(broker_stats, fun emqx_broker_helper:stats_fun/0).
%
% pr20000_ensure_sup_started(_FromVsn, "5.6.1" ++ _, ChildSpec) ->
%     ChildId = maps:get(id, ChildSpec),
%     case supervisor:terminate_child(emqx_sup, ChildId) of
%         ok -> supervisor:delete_child(emqx_sup, ChildId);
%         Error -> Error
%     end,
%     supervisor:start_child(emqx_sup, ChildSpec);
% pr20000_ensure_sup_started(_FromVsn, _TargetVsn, _) ->
%     ok.

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-export([pr_16802_restart_rabbitmq_connectors/1]).

-ignore_xref([
    {emqx_resource, list_instances_by_type, 1},
    {emqx_resource, get_instance, 1},
    {emqx_resource, restart, 1}
]).
pr_16802_restart_rabbitmq_connectors(_FromVsn) ->
    %% Need this hack because rebar3's xref check apparently ignores `ignore_xref`
    %% attribute for these calls....
    Mod = emqx_resource,
    ConnResIds0 = Mod:list_instances_by_type(emqx_bridge_rabbitmq_connector),
    ConnResIds =
        lists:filter(
            fun(ConnResId) ->
                case Mod:get_instance(ConnResId) of
                    {ok, _, #{status := stopped}} ->
                        false;
                    {ok, _, _} ->
                        true;
                    _ ->
                        false
                end
            end,
            ConnResIds0
        ),
    lists:foreach(fun Mod:restart/1, ConnResIds).
