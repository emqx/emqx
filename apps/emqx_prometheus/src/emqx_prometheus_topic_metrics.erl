%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_topic_metrics).
-moduledoc """
Prometheus collector for the v2 named-collection topic-metrics
feature. Exposes one counter series per (collection, counter) pair
with labels `name`, `topic_filter`, and `namespace`. Honors the same
`?mode=` query parameter as the sibling `/prometheus/*` endpoints
(`node` default, `all_nodes_aggregated`, `all_nodes_unaggregated`),
implemented via the `emqx_prometheus_cluster` behaviour. When
`collect_ns/2' sets a scope, the rendered row set is limited to that
owner namespace; otherwise every namespace is rendered.

v2 keeps plain counters only — there are no MMA-derived `.rate`
series. Consumers compute rates externally using `rate()`.
""".

%% Namespaced reads fan out through `emqx_prometheus_proto_v3'.
%% `fetch_from_local_node/1' is kept as the proto v2 RPC entry point for
%% peers that only advertise the older API.
-behaviour(emqx_prometheus_cluster).

%% prometheus uses this attribute to auto-register collectors.
-behaviour(prometheus_collector).

-include("emqx_prometheus.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_topic_metrics/include/emqx_topic_metrics.hrl").
-include_lib("prometheus/include/prometheus.hrl").

-define(REGISTRY, ?PROMETHEUS_TOPIC_METRICS_REGISTRY).
-define(MG(K, MAP), maps:get(K, MAP)).

%% data-key under which the per-metric points are stored in the map
%% returned by `fetch_namespaced_metrics_v1/2' (and by the legacy
%% `fetch_from_local_node/1' entry point).
-define(metrics_data_key, topic_metrics_data).

-export([
    registry/0,
    collect/1,
    collect_ns/2
]).

%% prometheus_collector callbacks
-export([
    collect_mf/2,
    collect_metrics/2,
    deregister_cleanup/1
]).

%% emqx_prometheus_cluster callbacks
-export([
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    fetch_namespaced_metrics_v1/2,
    fetch_cluster_wide_namespaced_metrics/2,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
]).

-import(prometheus_model_helpers, [create_mf/5, counter_metrics/1]).

%%--------------------------------------------------------------------
%% External API
%%
%% The collector is registered with `?REGISTRY' at boot via
%% `emqx_prometheus_config:register_collectors/0', so there is no
%% lifecycle function here — the collector is always available as
%% soon as the `emqx_prometheus' app has started.
%%--------------------------------------------------------------------

-spec registry() -> atom().
registry() -> ?REGISTRY.

-spec collect(binary()) -> binary().
collect(<<"prometheus">>) ->
    %% Entry point for callers that render this registry without a
    %% resolved namespace.  An already selected `?mode=' is preserved so
    %% the caller keeps getting the mode it asked for.
    Mode =
        case ?GET_PROM_DATA_MODE() of
            undefined -> ?PROM_DATA_MODE__NODE;
            Selected -> Selected
        end,
    collect_ns(all, Mode).

%% Render this registry for one namespace scope: `all' (every owner) or
%% a concrete owner namespace.  The scope is carried to `collect_mf/2'
%% in the process dictionary because the prometheus collector callback
%% arity is fixed.
-spec collect_ns(all | emqx_config:namespace(), atom()) -> binary().
collect_ns(Namespace, Mode) ->
    try
        ?PUT_PROM_DATA_MODE(Mode),
        put_namespace_pd(Namespace),
        prometheus_text_format:format(?REGISTRY)
    after
        _ = erase(?PROM_DATA_MODE_KEY__),
        _ = erase_namespace_pd()
    end.

%%--------------------------------------------------------------------
%% emqx_prometheus_cluster callbacks
%%--------------------------------------------------------------------

%% Legacy proto v2 RPC entry point: that API has no namespace concept,
%% so it returns points for every namespace.
fetch_from_local_node(Mode) ->
    fetch_namespaced_metrics_v1(all, Mode).

%% No cluster-consistent metadata for this endpoint — the row set
%% is per-node ETS, fan-out happens above us in the framework.
fetch_cluster_consistented_data() ->
    #{}.

-spec fetch_namespaced_metrics_v1(all | emqx_config:namespace(), atom()) -> {node(), map()}.
fetch_namespaced_metrics_v1(Namespace, Mode) ->
    Rows = emqx_topic_metrics2:list(registry_scope(Namespace)),
    {node(), #{
        ?metrics_data_key => to_points(Mode, Rows)
    }}.

%% Counters are per-node atomics, so there is nothing cluster-wide to
%% add to the rows collected from each node.
-spec fetch_cluster_wide_namespaced_metrics(all | emqx_config:namespace(), atom()) -> map().
fetch_cluster_wide_namespaced_metrics(_Namespace, _Mode) ->
    #{}.

aggre_or_zip_init_acc() ->
    #{
        ?metrics_data_key => maps:from_keys(metric_names(), [])
    }.

%% Every counter exposed here is a plain monotonic counter: summing
%% across nodes is the right reduction. No max-style "logic sum"
%% metrics.
logic_sum_metrics() ->
    [].

%%--------------------------------------------------------------------
%% prometheus_collector callbacks
%%--------------------------------------------------------------------

deregister_cleanup(_Registry) -> ok.

collect_mf(?REGISTRY, Callback) ->
    RawData = emqx_prometheus_cluster:raw_data_ns(
        ?MODULE, get_namespace_pd(), ?GET_PROM_DATA_MODE()
    ),
    Points = ?MG(?metrics_data_key, RawData),
    lists:foreach(
        fun(MetricName) ->
            Help = help_text(MetricName),
            Callback(create_mf(MetricName, Help, counter, ?MODULE, Points))
        end,
        metric_names()
    ),
    ok;
collect_mf(_Registry, _Callback) ->
    ok.

collect_metrics(MetricName, Points) ->
    counter_metrics(?MG(MetricName, Points)).

%%--------------------------------------------------------------------
%% Internal — namespace scope
%%--------------------------------------------------------------------

%% `all' is EMQX's "every namespace" scope, spelled `all_ns' by the
%% registry API.  A concrete owner (`?global_ns' or a tenant binary) is
%% passed through as-is: here `?global_ns' means "only global-owned
%% collections", unlike `emqx_topic_metrics2_api:list_scope/1', which
%% widens it to `all_ns' for a global admin.
registry_scope(all) -> all_ns;
registry_scope(?global_ns) -> ?global_ns;
registry_scope(Namespace) when is_binary(Namespace) -> Namespace.

put_namespace_pd(Namespace) when
    is_binary(Namespace);
    Namespace == ?global_ns;
    Namespace == all
->
    _ = put({?MODULE, ns}, Namespace),
    ok.

erase_namespace_pd() ->
    _ = erase({?MODULE, ns}),
    ok.

get_namespace_pd() ->
    get({?MODULE, ns}).

%%--------------------------------------------------------------------
%% Internal — sample construction
%%--------------------------------------------------------------------

%% Build the per-metric point map: one entry per metric name, value
%% is a list of `{Labels, CounterValue}' tuples (one per local row).
to_points(Mode, Rows) ->
    lists:foldl(
        fun(Rec, Acc) ->
            Labels = labels(Mode, Rec),
            #{metrics := Counts} = Rec,
            lists:foldl(
                fun(MetricAtom, Acc1) ->
                    MetricName = prometheus_metric_name(MetricAtom),
                    Value = ?MG(count_key(MetricAtom), Counts),
                    Acc1#{
                        MetricName => [{Labels, Value} | ?MG(MetricName, Acc1)]
                    }
                end,
                Acc,
                ?METRICS
            )
        end,
        maps:from_keys(metric_names(), []),
        Rows
    ).

labels(Mode, #{bin_name := BinName, topic_filter := TF, owner_ns := NS}) ->
    with_node_label(Mode, [
        {name, BinName},
        {topic_filter, TF}
        | [{namespace, NS} || is_binary(NS)]
    ]).

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node()} | Labels].

metric_names() ->
    [prometheus_metric_name(M) || M <- ?METRICS].

prometheus_metric_name('messages.in') -> emqx_topic_metric_messages_in_count;
prometheus_metric_name('messages.out') -> emqx_topic_metric_messages_out_count;
prometheus_metric_name('messages.dropped') -> emqx_topic_metric_messages_dropped_count;
prometheus_metric_name('bytes.in') -> emqx_topic_metric_bytes_in;
prometheus_metric_name('bytes.out') -> emqx_topic_metric_bytes_out.

%% The map returned by emqx_topic_metrics2:list/1 uses these atoms
%% as counter keys (matches the legacy `.count' suffix for messages;
%% the new `bytes.*' counters use the raw atom).
count_key('messages.in') -> 'messages.in.count';
count_key('messages.out') -> 'messages.out.count';
count_key('messages.dropped') -> 'messages.dropped.count';
count_key('bytes.in') -> 'bytes.in';
count_key('bytes.out') -> 'bytes.out'.

help_text(MetricName) ->
    iolist_to_binary(io_lib:format("Topic-metric collection ~ts counter.", [MetricName])).
