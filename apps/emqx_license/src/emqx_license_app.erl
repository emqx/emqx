%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_app).
-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-export([start/2, stop/1]).

%% Exported for tests: exercises the boot-time grace/retry logic without
%% starting the supervisor tree.
-export([validate_license/1]).

%% Grace window for the boot-time race where a single-node (community/default)
%% license node legitimately joins a cluster whose peers hold a clustering
%% license: cluster membership is established quickly, but the peer's license
%% only reaches this node later via config replication. During this window we
%% keep the current (still single-node) license as the last-seen error and poll
%% for the synced license before failing with SINGLE_NODE_LICENSE.
-define(DEFAULT_GRACE_PERIOD_MS, timer:seconds(30)).
-define(DEFAULT_GRACE_POLL_INTERVAL_MS, timer:seconds(1)).

start(_Type, _Args) ->
    Reader = fun emqx_license:read_license/0,
    case validate_license(Reader) of
        ok ->
            Tables = emqx_license_session_hwm:create_tables(),
            ok = mria:wait_for_tables(Tables),
            ok = emqx_license:load(),
            emqx_license_sup:start_link(Reader);
        {error, 'SINGLE_NODE_LICENSE'} ->
            {error,
                "SINGLE_NODE_LICENSE, make sure this node and peer nodes are configured with a valid license"}
    end.

stop(_State) ->
    ok = emqx_license:unload(),
    ok.

%% License violation check is done here (but not before boot)
%% because we must allow default single-node license to join cluster,
%% then check if the **fetched** license from peer node allows clustering.
validate_license(Reader) ->
    maybe
        {ok, License} ?= Reader(),
        ok ?= no_violation_within_grace(Reader, License)
    end.

%% A single-node license on an already-clustered node may be a transient
%% boot-time race (the peer's clustering license has not been replicated into
%% this node's config yet) rather than a genuine misconfiguration. Tolerate it
%% for a bounded grace window, re-reading the license, before failing. A truly
%% misconfigured cluster (all nodes single-node) never obtains a clustering
%% license and still fails, just after the grace window elapses.
no_violation_within_grace(Reader, License) ->
    case emqx_license_checker:no_violation(License) of
        ok ->
            ok;
        {error, 'SINGLE_NODE_LICENSE'} = Error ->
            GracePeriod = grace_period_ms(),
            ?SLOG(
                notice,
                #{
                    msg => "single_node_license_on_clustered_node_waiting_for_license_sync",
                    grace_period_ms => GracePeriod
                },
                #{tag => "LICENSE"}
            ),
            Deadline = erlang:monotonic_time(millisecond) + GracePeriod,
            wait_for_clustering_license(Reader, Error, Deadline)
    end.

wait_for_clustering_license(Reader, Error, Deadline) ->
    ok = timer:sleep(grace_poll_interval_ms()),
    case no_violation_once(Reader) of
        ok ->
            ?SLOG(
                notice,
                #{msg => "single_node_license_cleared_after_license_sync"},
                #{tag => "LICENSE"}
            ),
            ok;
        {error, _} ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    Error;
                false ->
                    wait_for_clustering_license(Reader, Error, Deadline)
            end
    end.

no_violation_once(Reader) ->
    maybe
        {ok, License} ?= Reader(),
        ok ?= emqx_license_checker:no_violation(License)
    end.

grace_period_ms() ->
    application:get_env(emqx_license, boot_grace_period_ms, ?DEFAULT_GRACE_PERIOD_MS).

grace_poll_interval_ms() ->
    application:get_env(emqx_license, boot_grace_poll_interval_ms, ?DEFAULT_GRACE_POLL_INTERVAL_MS).
