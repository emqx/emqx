%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_TOPIC_METRICS_HRL).
-define(EMQX_TOPIC_METRICS_HRL, true).

%% Local per-node ETS table that holds the counter_refs + a cached
%% copy of the cluster-replicated metadata (topic_filter, create_time,
%% reset_time). Keyed by Name :: {OwnerNs, BinName} so the same
%% bin-name can coexist in different namespaces without clashing.
-define(REGISTRY_TAB, emqx_topic_metrics_registry).

%% emqx_topic_index ETS table (ordered_set). Keys are made by
%% emqx_topic_index:make_key/2 from {TopicFilter, Name}. The record
%% body is the per-node counter_ref. Owner namespace is encoded in
%% the Name tuple, so the publish hook can filter by namespace
%% without a second lookup.
-define(INDEX_TAB, emqx_topic_metrics_index).

%% Mria table that is the cluster-wide source of truth for the v2
%% topic-metrics collection set. Persisted as `disc_copies' so the
%% set survives a full cluster restart without needing HOCON.
-define(MRIA_TAB, emqx_topic_metrics_mria).

%% Record stored in ?MRIA_TAB. The mnesia key is the {OwnerNs, BinName}
%% tuple in `name'. Counter refs are per-node and do NOT live in mria
%% — every node rehydrates a fresh counter_ref on boot. Reset events
%% are not persisted; each node emits its own audit log entry from
%% the cluster_rpc reset callback (see emqx_topic_metrics_registry).
-record(topic_metric, {
    name :: {'_' | global | binary(), '_' | binary()},
    topic_filter :: binary(),
    create_time :: binary()
}).

%% Owner namespace guard: either the global sentinel or a binary
%% tenant identifier. Use in function guards instead of the inline
%% `OwnerNs =:= ?global_ns orelse is_binary(OwnerNs)' check.
-define(IS_NAMESPACE(NS), (NS =:= ?global_ns orelse is_binary(NS))).

%% Per-node cap on the number of registered collections.
%% Must stay in sync with the legacy ?MAX_TOPICS in
%% apps/emqx_modules/src/emqx_topic_metrics.erl until the legacy module is
%% removed. The user explicitly chose to reuse the legacy cap value for v2.
-define(MAX_COLLECTIONS, 512).

%% Ordered list of counter names. The position in this list is the
%% atomics index used in counters:add/3 / counters:get/2. Per-QoS
%% counters are intentionally omitted — they can be added back if a
%% customer requests them. `bytes.in' and `bytes.out' track payload
%% throughput so operators can size networks and reason about quota
%% beyond just message counts.
-define(METRICS, [
    'messages.in',
    'messages.out',
    'messages.dropped',
    'bytes.in',
    'bytes.out'
]).

%% Name validation regex. See emqx_topic_metrics_schema for the schema-level
%% binding. Allowed characters per req: a-zA-Z0-9-_. Length 1..64.
-define(NAME_REGEX, <<"^[a-zA-Z0-9_-]{1,64}$">>).

-endif.
