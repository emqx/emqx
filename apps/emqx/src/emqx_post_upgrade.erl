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

-export([
    pr_17586_kickoff_registry_keeper/1,
    pr_17625_gcp_pubsub_consumer_worker_optvars/1
]).

%% Replicants started on the pre-fix beam stored {no_deletes => true} in
%% the keeper's state and never scheduled a sweep timer. After the new
%% beam is loaded the process still has that state shape; this hook
%% sends `start' so handle_info/2 runs ensure_sweep_keys/1 and begins
%% the periodic local-pid sweep. Idempotent on cores (where a timer is
%% already running) — just brings the next tick forward.
pr_17586_kickoff_registry_keeper(_FromVsn) ->
    emqx_cm_registry_keeper:ensure_started().

-define(GCONSU_WORKER_OPT_KEY(X), {emqx_bridge_gcp_pubsub_consumer_worker, subscription_ok, X}).
pr_17625_gcp_pubsub_consumer_worker_optvars(_FromVsn) ->
    %% Need this hack because rebar3's xref check apparently ignores `ignore_xref`
    %% attribute for these calls....
    EMQXResource = emqx_resource,
    ECPool = ecpool,
    ECPoolWorker = ecpool_worker,
    ConnImpl = emqx_bridge_gcp_pubsub_impl_consumer,
    ConnResIds0 = EMQXResource:list_instances_by_type(ConnImpl),
    HasOldOptvars1 = fun(SourceResId) ->
        lists:any(
            fun(Pid) ->
                optvar:is_set(?GCONSU_WORKER_OPT_KEY(Pid))
            end,
            [
                Pid
             || {_, EWPid} <- ECPool:workers(SourceResId),
                {ok, Pid} <- [ECPoolWorker:client(EWPid)]
            ]
        )
    end,
    HasOldOptvars = fun(ConnResId) ->
        SourceResIds =
            lists:map(
                fun({Id, _}) -> Id end,
                ConnImpl:on_get_channels(ConnResId)
            ),
        lists:any(HasOldOptvars1, SourceResIds)
    end,
    ConnResIds =
        lists:filter(
            fun(ConnResId) ->
                case EMQXResource:get_instance(ConnResId) of
                    {ok, _, #{status := stopped}} ->
                        false;
                    {ok, _, _} ->
                        HasOldOptvars(ConnResId);
                    _ ->
                        false
                end
            end,
            ConnResIds0
        ),
    lists:foreach(fun EMQXResource:restart/1, ConnResIds),
    lists:foreach(
        fun
            (?GCONSU_WORKER_OPT_KEY(Pid) = K) when is_pid(Pid) ->
                optvar:unset(K);
            (_) ->
                ok
        end,
        optvar:list_all()
    ),
    ok.
