%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics2_proto_v1).
-moduledoc """
bpapi for the v2 topic-metrics feature.

Durable cluster state lives in a mria `disc_copies` table, keyed by
`{OwnerNs, BinName}`. The facade (`emqx_topic_metrics2`) writes the
mria row inside a `mria:transaction` BEFORE invoking the cluster_rpc
multicall wrappers below. The cluster_rpc callbacks themselves never
touch mria — they only update the local ETS overlay
(`emqx_topic_metrics_registry:do_install_local/3`,
`do_uninstall_local/1`, `do_reset_local/1`,
`do_uninstall_all_local/1`).

Read operations use `emqx_rpc:multicall` and the caller aggregates
the per-node results.
""".

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    install_local/3,
    uninstall_local/1,
    reset_local/1,
    uninstall_all_local/1,

    list/2,
    lookup/2
]).

introduced_in() ->
    "6.3.0".

%%--------------------------------------------------------------------
%% Cluster-coordinated LOCAL side-effect writes (via emqx_cluster_rpc).
%% The mria row has already been persisted by the facade before any
%% of these is invoked. `Name' is the qualified `{OwnerNs, BinName}'
%% tuple — owner namespace is encoded in the key.
%%--------------------------------------------------------------------

-spec install_local(emqx_topic_metrics_registry:name(), binary(), binary()) -> ok.
install_local(Name, TopicFilter, CreateTime) ->
    emqx_cluster_rpc:multicall(
        emqx_topic_metrics_registry,
        do_install_local,
        [Name, TopicFilter, CreateTime]
    ).

-spec uninstall_local(emqx_topic_metrics_registry:name()) -> ok.
uninstall_local(Name) ->
    emqx_cluster_rpc:multicall(
        emqx_topic_metrics_registry, do_uninstall_local, [Name]
    ).

-spec reset_local(emqx_topic_metrics_registry:name()) -> ok.
reset_local(Name) ->
    emqx_cluster_rpc:multicall(
        emqx_topic_metrics_registry, do_reset_local, [Name]
    ).

-spec uninstall_all_local([emqx_topic_metrics_registry:name()]) -> ok.
uninstall_all_local(Names) ->
    emqx_cluster_rpc:multicall(
        emqx_topic_metrics_registry, do_uninstall_all_local, [Names]
    ).

%%--------------------------------------------------------------------
%% Read-side cluster fan-out (via emqx_rpc)
%%--------------------------------------------------------------------

-spec list([node()], emqx_topic_metrics_registry:owner_ns() | all_ns) ->
    emqx_rpc:multicall_result().
list(Nodes, OwnerNs) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics2, list, [OwnerNs]).

-spec lookup([node()], emqx_topic_metrics_registry:name()) ->
    emqx_rpc:multicall_result().
lookup(Nodes, Name) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics2, lookup, [Name]).
