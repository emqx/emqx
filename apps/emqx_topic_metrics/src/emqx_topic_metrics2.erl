%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics2).
-moduledoc """
Public API for the v2 named-collection topic-metrics feature.

Collections are addressed by a `{OwnerNs, BinName}` qualified key.
The facade hides the qualification from callers that already know
their namespace — `register(BinName, TF, NS)` /
`lookup(BinName, NS)` / `deregister(BinName, NS)` all take the
user-facing bin-name plus the actor's namespace and build the
qualified key internally.

Each write operation is a two-step dance:

  1. Persist the durable cluster state in mria via a
     `mria:transaction` on the initiator node. The transaction
     bundles the cap / duplicate check with the `mnesia:write` or
     `mnesia:delete` so the operation is atomic cluster-wide.

  2. Fan out the corresponding LOCAL ETS side-effect to every
     cluster node via `emqx_cluster_rpc:multicall` (wrapped by
     `emqx_topic_metrics2_proto_v1`). The cluster_rpc callback
     receives the data it needs in its args and never touches mria;
     mria's own replication is consulted only on boot, by
     `emqx_topic_metrics_registry:rehydrate/0`.

Reads are local ETS lookups; the REST API fans them out across the
cluster via the read-side proto when it wants aggregated counters.
""".

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_topic_metrics.hrl").

%% Lifecycle (called from emqx_topic_metrics_app).
-export([
    load/0,
    unload/0
]).

%% Facade.
-export([
    register/3,
    deregister/2,
    deregister_all/0,
    deregister_all/1,
    reset/2,
    lookup/1,
    lookup/2,
    list/1
]).

-type owner_ns() :: emqx_topic_metrics_registry:owner_ns().
-type bin_name() :: emqx_topic_metrics_registry:bin_name().
-type name() :: emqx_topic_metrics_registry:name().

%%--------------------------------------------------------------------
%% Lifecycle
%%--------------------------------------------------------------------

-spec load() -> ok.
load() ->
    %% Hooks are installed after the registry has rehydrated from
    %% mria (which happens synchronously in
    %% emqx_topic_metrics_registry:init/1). The supervisor brings
    %% the registry up before this call returns.
    ok = emqx_topic_metrics_hooks:enable(),
    ok.

-spec unload() -> ok.
unload() ->
    ok = emqx_topic_metrics_hooks:disable(),
    ok.

%%--------------------------------------------------------------------
%% Facade — cluster-coordinated writes
%%--------------------------------------------------------------------

-spec register(bin_name(), binary(), owner_ns()) ->
    ok | {error, already_registered | quota_exceeded | term()}.
register(BinName, TopicFilter, OwnerNs) when is_binary(BinName), is_binary(TopicFilter) ->
    case validate_inputs(BinName, TopicFilter) of
        ok ->
            CreateTime = emqx_utils_calendar:now_to_rfc3339(),
            Name = {check_ns(OwnerNs), BinName},
            case emqx_topic_metrics_registry:persist_register(Name, TopicFilter, CreateTime) of
                {ok, new} ->
                    %% Durable row is in mria; tell every node to
                    %% materialise the local ETS overlay.
                    _ = emqx_topic_metrics2_proto_v1:install_local(Name, TopicFilter, CreateTime),
                    ok;
                {ok, existing} ->
                    %% Idempotent — the row was already in mria.
                    %% Each node's ETS overlay was installed on the
                    %% prior register, so nothing to broadcast.
                    ok;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec deregister(bin_name(), owner_ns()) -> ok | {error, not_found}.
deregister(BinName, OwnerNs) when is_binary(BinName) ->
    Name = {check_ns(OwnerNs), BinName},
    case emqx_topic_metrics_registry:persist_deregister(Name) of
        {ok, gone} ->
            _ = emqx_topic_metrics2_proto_v1:uninstall_local(Name),
            ok;
        {error, _} = Err ->
            Err
    end.

-spec deregister_all() -> ok.
deregister_all() ->
    deregister_all(all_ns).

%% Scope: `all_ns' wipes every collection cluster-wide. A namespace
%% value (`?global_ns' or a binary tenant) wipes only collections
%% owned by that namespace.
-spec deregister_all(owner_ns() | all_ns) -> ok.
deregister_all(Scope) ->
    NormScope = check_scope(Scope),
    {ok, Names} = emqx_topic_metrics_registry:persist_deregister_all_owned_by(NormScope),
    case Names of
        [] ->
            ok;
        _ ->
            _ = emqx_topic_metrics2_proto_v1:uninstall_all_local(Names),
            ok
    end.

-spec reset(bin_name(), owner_ns()) -> ok | {error, not_found}.
reset(BinName, OwnerNs) when is_binary(BinName) ->
    Name = {check_ns(OwnerNs), BinName},
    %% Reset is not persisted in mria — counters are per-node atomics
    %% recreated at every boot, so a stored "reset_time" would lie
    %% after the next restart. We do a local existence check so the
    %% REST handler can return 404 for unknown names, then broadcast
    %% the side-effect. Each node emits its own audit log entry in
    %% emqx_topic_metrics_registry:do_reset_local/2.
    case emqx_topic_metrics_registry:lookup(Name) of
        {error, not_found} ->
            {error, not_found};
        {ok, _} ->
            _ = emqx_topic_metrics2_proto_v1:reset_local(Name),
            ok
    end.

%%--------------------------------------------------------------------
%% Facade — local reads
%%--------------------------------------------------------------------

-spec lookup(bin_name(), owner_ns()) -> {ok, map()} | {error, not_found}.
lookup(BinName, OwnerNs) when is_binary(BinName) ->
    lookup({check_ns(OwnerNs), BinName}).

%% Variant that takes the qualified `{OwnerNs, BinName}' key
%% directly. Used by the read-side proto module so per-node lookups
%% don't have to re-split the namespace from the name.
-spec lookup(name()) -> {ok, map()} | {error, not_found}.
lookup({_OwnerNs, BinName} = Name) when is_binary(BinName) ->
    case emqx_topic_metrics_registry:lookup(Name) of
        {ok, Rec} -> {ok, with_counters(Rec)};
        {error, _} = Err -> Err
    end.

-spec list(owner_ns() | all_ns) -> [map()].
list(Scope) ->
    [with_counters(Rec) || Rec <- emqx_topic_metrics_registry:list(check_scope(Scope))].

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

counters_snapshot(CRef) ->
    {_, Map} =
        lists:foldl(
            fun(Metric, {Idx, Acc}) ->
                {Idx + 1, Acc#{count_key(Metric) => counters:get(CRef, Idx)}}
            end,
            {1, #{}},
            ?METRICS
        ),
    Map.

validate_inputs(BinName, TopicFilter) ->
    case emqx_topic_metrics_schema:validate_name(BinName) of
        ok -> emqx_topic_metrics_schema:validate_topic_filter(TopicFilter);
        {error, _} = Err -> Err
    end.

check_ns(?global_ns) -> ?global_ns;
check_ns(NS) when is_binary(NS) -> NS.

check_scope(all_ns) -> all_ns;
check_scope(?global_ns) -> ?global_ns;
check_scope(NS) when is_binary(NS) -> NS.

with_counters(#{counter_ref := CRef} = Rec) ->
    Rec#{metrics => counters_snapshot(CRef)}.

%% Match the legacy `.count' suffix so a downstream user that wants to
%% switch between v1 and v2 only changes the URL, not field names.
count_key('messages.in') -> 'messages.in.count';
count_key('messages.out') -> 'messages.out.count';
count_key('messages.dropped') -> 'messages.dropped.count';
%% Bytes counters are new in v2 (legacy v1 didn't expose them), so
%% there is no legacy-suffix convention to match — we use the raw
%% atom as-is.
count_key('bytes.in') -> 'bytes.in';
count_key('bytes.out') -> 'bytes.out'.
