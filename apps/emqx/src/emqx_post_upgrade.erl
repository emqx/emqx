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
    pr_17586_kickoff_registry_keeper/1
]).

%% Replicants started on the pre-fix beam stored {no_deletes => true} in
%% the keeper's state and never scheduled a sweep timer. After the new
%% beam is loaded the process still has that state shape; this hook
%% sends `start' so handle_info/2 runs ensure_sweep_keys/1 and begins
%% the periodic local-pid sweep. Idempotent on cores (where a timer is
%% already running) — just brings the next tick forward.
pr_17586_kickoff_registry_keeper(_FromVsn) ->
    emqx_cm_registry_keeper:ensure_started().
