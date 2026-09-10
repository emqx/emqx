%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_sup).

-behaviour(supervisor).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, init/1]).
-export([restart_pools/1]).
-export([start_pool/2]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

restart_pools(PoolSize) ->
    %% Ask pull_shard to stop flushing new batches and snapshot inflight
    %% claim marks atomically. No mark can slip in between this snapshot and
    %% worker termination. pull_shard survives the restart and replays these
    %% generations after the new pools are up.
    case emqx_bcast_pull_shard:begin_pools_restart() of
        {error, restart_in_progress} ->
            ?SLOG(warning, #{
                msg => "bcast_pools_restart_already_in_progress"
            }),
            ok;
        {ok, Marks} ->
            Results = [
                restart_pool_child(ChildId, PoolSize)
             || ChildId <- [
                    bcast_pull_worker_pool_sup,
                    bcast_pull_server_pool_sup
                ]
            ],
            lists:foreach(
                fun
                    (ok) ->
                        ok;
                    ({error, ChildId, Reason}) ->
                        ?SLOG(warning, #{
                            msg => "bcast_pool_restart_failed",
                            child => ChildId,
                            reason => Reason
                        })
                end,
                Results
            ),
            %% Even if one pool failed to restart, clear/release the marks
            %% that were in flight while the workers were being killed.
            emqx_bcast_pull_shard:worker_pools_restarted(Marks),
            ok
    end.

restart_pool_child(ChildId, PoolSize) ->
    case lists:keymember(ChildId, 1, supervisor:which_children(?MODULE)) of
        true ->
            case supervisor:terminate_child(?MODULE, ChildId) of
                ok ->
                    case supervisor:delete_child(?MODULE, ChildId) of
                        ok ->
                            case supervisor:start_child(?MODULE, pool_spec(ChildId, PoolSize)) of
                                {ok, _Pid} ->
                                    ok;
                                {error, Reason} ->
                                    {error, ChildId, {start_failed, Reason}}
                            end;
                        {error, Reason} ->
                            {error, ChildId, {delete_failed, Reason}}
                    end;
                {error, Reason} ->
                    {error, ChildId, {terminate_failed, Reason}}
            end;
        false ->
            ok
    end.

init([]) ->
    %% Recreate bcast_device_registry before any child or hook can register a
    %% device. This init path is the backstop after an application/supervisor
    %% restart; normal ownership follows whichever OTP process executes init.
    ok = ensure_device_registry_table(),
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 3600},
    PoolSize = pool_size(),
    Core = emqx_bcast:is_core(),
    %% Ack accumulation is sharded per client partition (emqx_bcast_ack_shard);
    %% the actual core accounting still runs in pull_server_pool's short-lived
    %% ack workers bounded by its own ack_cap (=schedulers) because each ack
    %% batch needs a completion notification back to the coordinator
    %% (round-robin emqx_pool tasks have no per-task callback).
    Children =
        [
            pool_spec(bcast_pull_worker_pool_sup, PoolSize)
        ] ++
            core_pool_specs(Core, PoolSize) ++
            [
                #{
                    id => {emqx_bcast_pull_shard, Shard},
                    start => {emqx_bcast_pull_shard, start_link, [Shard]},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [emqx_bcast_pull_shard]
                }
             || Shard <- lists:seq(0, emqx_bcast_pull_shard:shard_count() - 1)
            ] ++
            [
                #{
                    id => {emqx_bcast_ack_shard, Shard},
                    start => {emqx_bcast_ack_shard, start_link, [Shard]},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [emqx_bcast_ack_shard]
                }
             || Shard <- lists:seq(0, emqx_bcast_ack_shard:shard_count() - 1)
            ] ++
            core_children(Core),
    {ok, {SupFlags, Children}}.

core_pool_specs(true, PoolSize) ->
    [pool_spec(bcast_pull_server_pool_sup, PoolSize)];
core_pool_specs(false, _PoolSize) ->
    [].

core_children(true) ->
    [
        #{
            id => emqx_bcast_pull_server_pool,
            start => {emqx_bcast_pull_server_pool, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_pull_server_pool]
        },
        #{
            id => emqx_bcast_cleanup,
            start => {emqx_bcast_cleanup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_cleanup]
        },
        %% Intake queue must start before the promoter drains it.
        #{
            id => emqx_bcast_intake,
            start => {emqx_bcast_intake, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_intake]
        },
        #{
            id => emqx_bcast_promoter,
            start => {emqx_bcast_promoter, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_promoter]
        }
    ] ++
        [
            #{
                id => {emqx_bcast_index_owner, Shard},
                start => {emqx_bcast_index_owner, start_link, [Shard]},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_bcast_index_owner]
            }
         || Shard <- lists:seq(0, emqx_bcast_index_owner:shard_count() - 1)
        ];
core_children(false) ->
    [].

ensure_device_registry_table() ->
    emqx_bcast_utils:ensure_ets(?TAB_DEV_REGISTRY, ?BCAST_DEV_REGISTRY_OPTS).

pool_size() ->
    emqx_bcast_config:get(delivery_pool_size).

pool_spec(ChildId, PoolSize) ->
    PoolName =
        case ChildId of
            bcast_pull_worker_pool_sup -> emqx_bcast_pull_worker_pool;
            bcast_pull_server_pool_sup -> emqx_bcast_pull_server_worker_pool
        end,
    #{
        id => ChildId,
        start => {?MODULE, start_pool, [PoolName, PoolSize]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [emqx_pool_sup]
    }.

%% gproc pool registrations survive supervisor teardown: a stale pool kept
%% at the old size makes emqx_pool_sup:init's ensure_pool_worker throw
%% out_of_range when the replacement pool is larger (reproduced online: a
%% delivery_pool_size 16->64 hot reload and a plugin restart with the
%% stored config still at 64 both died in bcast_pull_worker_pool_sup with
%% {out_of_range, gproc_pool:add_worker}). Force-delete the registration
%% before every (re)start - the same pattern emqx_resource_buffer_worker_sup
%% and emqx_ds_beamformer use - so both first start and pool resizing work.
start_pool(PoolName, PoolSize) ->
    _ = catch gproc_pool:force_delete(PoolName),
    emqx_pool_sup:start_link(PoolName, round_robin, PoolSize, {emqx_pool, start_link, []}).
