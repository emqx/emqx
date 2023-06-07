-module(emqx_prometheus_json_format).

-export([
    content_type/0,
    format/0,
    format/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("prometheus/include/prometheus_model.hrl").

-behaviour(prometheus_format).

-define(TAB, ?MODULE).

%%====================================================================
%% Macros
%%====================================================================

%%====================================================================
%% Format API
%%====================================================================

-spec content_type() -> binary().
%% @doc
%% Returns content type of custom json format.
%% @end
content_type() ->
    <<"application/json">>.

%% @equiv format(default)
-spec format() -> binary().
%% @doc
%% Formats `default' registry using the latest text format.
%% @end
format() ->
    format(default).

-spec format(Registry :: prometheus_registry:registry()) -> binary().
%% @doc
%% Formats `Registry' using the latest text format.
%% @end
format(Registry) ->
    _ =
        case ets:whereis(?TAB) of
            undefined ->
                _Tab = ets:new(?TAB, [bag, named_table, public, {write_concurrency, true}]);
            _Ref ->
                ets:delete_all_objects(?TAB)
        end,
    ok = prometheus_registry:collect(
        Registry,
        fun(_Registry, Collector) ->
            prometheus_collector:collect_mf(
                Registry,
                Collector,
                fun store_mf_metrics/1
            )
        end
    ),
    Result = ets:foldl(fun aggregate_tab/2, #{}, ?TAB),
    ?SLOG(
        debug,
        #{
            msg => emqx_prometheus_json_format_result,
            registry => Registry,
            result => Result
        }
    ),
    emqx_utils_json:encode(Result).

%%====================================================================
%% Private Parts
%%====================================================================
aggregate_tab({K, V0}, M) ->
    V = to_map(V0),
    case maps:get(K, M, undefined) of
        undefined ->
            M#{K => V};
        OldValues when is_list(OldValues) ->
            M#{K => [V | OldValues]};
        OldValue ->
            M#{K => [V, OldValue]}
    end.

to_map(V) when is_list(V) ->
    maps:from_list(V);
to_map(V) ->
    V.

%% @private
store_mf_metrics(#'MetricFamily'{name = Name, metric = Metrics}) ->
    lists:foreach(
        fun(Metric) ->
            store_metric(Name, Metric)
        end,
        Metrics
    ).

store_metric(Name, #'Metric'{
    label = Labels,
    counter = #'Counter'{value = Value}
}) ->
    store_series(Name, label_pairs_to_proplist(Labels), Value);
store_metric(Name, #'Metric'{
    label = Labels,
    gauge = #'Gauge'{value = Value}
}) ->
    store_series(Name, label_pairs_to_proplist(Labels), Value);
store_metric(Name, #'Metric'{
    label = Labels,
    untyped = #'Untyped'{value = Value}
}) ->
    store_series(Name, label_pairs_to_proplist(Labels), Value);
store_metric(Name, #'Metric'{
    label = Labels,
    summary = #'Summary'{
        sample_count = Count,
        sample_sum = Sum,
        quantile = Quantiles
    }
}) ->
    LabelsList = label_pairs_to_proplist(Labels),
    store_series([Name, "_count"], LabelsList, Count),
    store_series([Name, "_sum"], LabelsList, Sum),
    [
        store_series(
            [Name],
            LabelsList ++
                label_pairs_to_proplist([
                    #'LabelPair'{name = "quantile", value = io_lib:format("~p", [QN])}
                ]),

            QV
        )
     || #'Quantile'{quantile = QN, value = QV} <- Quantiles
    ];
store_metric(Name, #'Metric'{
    label = Labels,
    histogram = #'Histogram'{
        sample_count = Count,
        sample_sum = Sum,
        bucket = Buckets
    }
}) ->
    LabelsList = label_pairs_to_proplist(Labels),
    [store_histogram_bucket(Name, LabelsList, Bucket) || Bucket <- Buckets],
    store_series([Name, "_count"], LabelsList, Count),
    store_series([Name, "_sum"], LabelsList, Sum).

store_histogram_bucket(Name, LabelsList, #'Bucket'{
    cumulative_count = BCount,
    upper_bound = BBound
}) ->
    BLValue = bound_to_label_value(BBound),
    store_series(
        [Name, "_bucket"],
        LabelsList ++
            label_pairs_to_proplist([#'LabelPair'{name = "le", value = BLValue}]),

        BCount
    ).

label_pairs_to_proplist(Labels) ->
    Fun = fun(#'LabelPair'{name = Name, value = Value}) ->
        {Name, Value}
    end,
    lists:map(Fun, Labels).

store_series(_Name, _LString, undefined) ->
    ok;
store_series(Name, [], Value) ->
    ets:insert(?TAB, {to_key([Name], []), Value});
store_series(Name, LString, Value) ->
    ets:insert(?TAB, {to_key([Name], []), [{value, Value} | LString]}).

to_key([], All) ->
    list_to_binary(lists:flatten(lists:reverse(All)));
to_key([H | T], All) when is_binary(H) ->
    to_key(T, [binary_to_list(H) | All]);
to_key([H | T], All) ->
    to_key(T, [H | All]).

bound_to_label_value(Bound) when is_integer(Bound) ->
    integer_to_list(Bound);
bound_to_label_value(Bound) when is_float(Bound) ->
    float_to_list(Bound);
bound_to_label_value(infinity) ->
    "+Inf".
